//! Moving entries and data through the shared region.
//!
//! The peer can write any byte at any time, so every access here is an
//! atomic, never a plain reference or `copy_nonoverlapping`. A torn entry is
//! then only bad input for the checks. Each side keeps its own index in
//! private memory and never reads its own field back.

#![allow(unsafe_code)]

use std::any::Any;
use std::fmt;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering, fence};

use crate::error::{Broken, LayoutError};
use crate::layout::{CQE_BYTES, Geometry, HEADER_BYTES, Header, PAGE, SQE_BYTES, control};

/// A mapped region. All access goes through atomics.
pub struct Region {
    base: NonNull<u8>,
    geometry: Geometry,
    // Keeps the mapping alive as long as the last ring handle.
    _owner: Option<Box<dyn Any + Send + Sync>>,
}

impl fmt::Debug for Region {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Region")
            .field("base", &self.base)
            .field("geometry", &self.geometry)
            .finish_non_exhaustive()
    }
}

// SAFETY: only atomic access, to memory that stays mapped while the Region
// lives.
unsafe impl Send for Region {}
// SAFETY: as for Send; shared references only allow atomic access.
unsafe impl Sync for Region {}

impl Region {
    /// Wraps a mapping of the region.
    ///
    /// # Safety
    ///
    /// - `base` is the start of a mapping at least `len` bytes long, readable
    ///   and writable, that stays mapped while this Region and every handle
    ///   made from it live.
    /// - In this process, nothing else reads or writes those bytes except
    ///   through this Region. Another process may write them at any time.
    pub unsafe fn from_raw_parts(
        base: *mut u8,
        len: usize,
        geometry: Geometry,
    ) -> Result<Self, LayoutError> {
        let base = NonNull::new(base).ok_or(LayoutError::Mapping)?;
        if !(base.as_ptr() as usize).is_multiple_of(PAGE) || len < geometry.region_len() {
            return Err(LayoutError::Mapping);
        }
        Ok(Self {
            base,
            geometry,
            _owner: None,
        })
    }

    /// As [`Region::from_raw_parts`], but holds `owner` (which may unmap on
    /// drop) until the last handle is gone.
    ///
    /// # Safety
    ///
    /// As for [`Region::from_raw_parts`], except that the mapping must stay
    /// valid only until `owner` is dropped.
    pub unsafe fn with_owner(
        base: *mut u8,
        len: usize,
        geometry: Geometry,
        owner: impl Any + Send + Sync,
    ) -> Result<Self, LayoutError> {
        // SAFETY: the caller's guarantee; the Region holds `owner` until it
        // drops.
        let mut r = unsafe { Self::from_raw_parts(base, len, geometry) }?;
        r._owner = Some(Box::new(owner));
        Ok(r)
    }

    /// The geometry the region was made with.
    pub fn geometry(&self) -> &Geometry {
        &self.geometry
    }

    fn u32_at(&self, off: usize) -> &AtomicU32 {
        debug_assert!(off.is_multiple_of(4) && off + 4 <= self.geometry.region_len());
        // SAFETY: `off` comes from the checked geometry, so it is in the
        // mapping and 4-byte aligned on a page-aligned base. The mapping
        // outlives `&self` and is only used through atomics.
        unsafe { AtomicU32::from_ptr(self.base.as_ptr().add(off).cast()) }
    }

    fn u64_at(&self, off: usize) -> &AtomicU64 {
        debug_assert!(off.is_multiple_of(8) && off + 8 <= self.geometry.region_len());
        // SAFETY: as in `u32_at`, with 8-byte alignment.
        unsafe { AtomicU64::from_ptr(self.base.as_ptr().add(off).cast()) }
    }

    /// `out.len()` and `off` must be multiples of 8 and inside the region.
    fn load(&self, off: usize, out: &mut [u8]) {
        for (i, chunk) in out.chunks_exact_mut(8).enumerate() {
            let v = self.u64_at(off + i * 8).load(Ordering::Relaxed);
            chunk.copy_from_slice(&v.to_ne_bytes());
        }
    }

    /// Same rules as `load`.
    fn store(&self, off: usize, data: &[u8]) {
        for (i, chunk) in data.chunks_exact(8).enumerate() {
            let mut v = [0u8; 8];
            v.copy_from_slice(chunk);
            self.u64_at(off + i * 8)
                .store(u64::from_ne_bytes(v), Ordering::Relaxed);
        }
    }

    /// The engine calls this once, before it sends the region.
    pub fn write_header(&self, h: &Header) {
        self.store(0, &h.encode());
    }

    /// rust-bhyve calls this once at attach.
    pub fn read_header(&self) -> Result<Header, LayoutError> {
        let mut b = [0u8; HEADER_BYTES];
        self.load(0, &mut b);
        Header::decode(&b)
    }

    /// Returns false if the range is outside queue `q`'s buffer area or the
    /// length is not a multiple of 8.
    pub fn read_buffer(&self, q: u16, page: u32, out: &mut [u8]) -> bool {
        match self.buffer_range(q, page, out.len()) {
            Some(off) => {
                self.load(off, out);
                true
            }
            None => false,
        }
    }

    /// Same rules as [`Region::read_buffer`].
    pub fn write_buffer(&self, q: u16, page: u32, data: &[u8]) -> bool {
        match self.buffer_range(q, page, data.len()) {
            Some(off) => {
                self.store(off, data);
                true
            }
            None => false,
        }
    }

    fn buffer_range(&self, q: u16, page: u32, len: usize) -> Option<usize> {
        let g = &self.geometry;
        if q >= g.queues() || !len.is_multiple_of(8) {
            return None;
        }
        let start = (page as usize).checked_mul(PAGE)?;
        let end = start.checked_add(len)?;
        if end > g.buf_pages() as usize * PAGE {
            return None;
        }
        Some(g.buf_offset(q) + start)
    }

    fn ends(
        self: &Arc<Self>,
        q: u16,
        ring: Ring,
        own: usize,
        peer: usize,
        idle: usize,
    ) -> Option<Ends> {
        let g = &self.geometry;
        if q >= g.queues() {
            return None;
        }
        let ctl = g.queue_offset(q);
        let entries = match ring {
            Ring::Sq => g.sq_offset(q),
            Ring::Cq => g.cq_offset(q),
        };
        Some(Ends {
            region: Arc::clone(self),
            entries,
            mask: g.depth() - 1,
            depth: g.depth(),
            own: ctl + own,
            peer: ctl + peer,
            idle: ctl + idle,
        })
    }

    /// rust-bhyve's end of queue `q`'s submission ring.
    pub fn sq_producer(self: &Arc<Self>, q: u16) -> Option<Producer<SQE_BYTES>> {
        use control::{ENGINE_IDLE, SQ_HEAD, SQ_TAIL};
        self.ends(q, Ring::Sq, SQ_TAIL, SQ_HEAD, ENGINE_IDLE)
            .map(Producer::new)
    }

    /// The engine's end of queue `q`'s submission ring.
    pub fn sq_consumer(self: &Arc<Self>, q: u16) -> Option<Consumer<SQE_BYTES>> {
        use control::{ENGINE_IDLE, SQ_HEAD, SQ_TAIL};
        self.ends(q, Ring::Sq, SQ_HEAD, SQ_TAIL, ENGINE_IDLE)
            .map(Consumer::new)
    }

    /// The engine's end of queue `q`'s completion ring.
    pub fn cq_producer(self: &Arc<Self>, q: u16) -> Option<Producer<CQE_BYTES>> {
        use control::{CQ_HEAD, CQ_TAIL, VMM_IDLE};
        self.ends(q, Ring::Cq, CQ_TAIL, CQ_HEAD, VMM_IDLE)
            .map(Producer::new)
    }

    /// rust-bhyve's end of queue `q`'s completion ring.
    pub fn cq_consumer(self: &Arc<Self>, q: u16) -> Option<Consumer<CQE_BYTES>> {
        use control::{CQ_HEAD, CQ_TAIL, VMM_IDLE};
        self.ends(q, Ring::Cq, CQ_HEAD, CQ_TAIL, VMM_IDLE)
            .map(Consumer::new)
    }
}

#[derive(Debug, Clone, Copy)]
enum Ring {
    Sq,
    Cq,
}

/// Offsets one end of a ring needs. For a producer, `own` is the tail,
/// `peer` the head, and `idle` the consumer's flag; for a consumer, `own` is
/// the head, `peer` the tail, and `idle` its own flag.
#[derive(Debug)]
struct Ends {
    region: Arc<Region>,
    entries: usize,
    mask: u32,
    depth: u32,
    own: usize,
    peer: usize,
    idle: usize,
}

impl Ends {
    fn slot<const N: usize>(&self, index: u32) -> usize {
        self.entries + (index & self.mask) as usize * N
    }
}

/// The producing end of one ring. Make at most one per ring.
#[derive(Debug)]
pub struct Producer<const N: usize> {
    ends: Ends,
    tail: u32,
    head: u32,
}

impl<const N: usize> Producer<N> {
    fn new(ends: Ends) -> Self {
        Self {
            ends,
            tail: 0,
            head: 0,
        }
    }

    /// Writes one entry after the last one. Returns `Ok(false)` if the ring
    /// is full. The entry is not visible until [`Producer::publish`].
    pub fn try_push(&mut self, entry: &[u8; N]) -> Result<bool, Broken> {
        if self.tail.wrapping_sub(self.head) == self.ends.depth {
            let h = self
                .ends
                .region
                .u32_at(self.ends.peer)
                .load(Ordering::Acquire);
            // The consumer's head can only move forward, up to our tail.
            if h.wrapping_sub(self.head) > self.tail.wrapping_sub(self.head) {
                return Err(Broken);
            }
            self.head = h;
            if self.tail.wrapping_sub(self.head) == self.ends.depth {
                return Ok(false);
            }
        }
        let slot = self.ends.slot::<N>(self.tail);
        self.ends.region.store(slot, entry);
        self.tail = self.tail.wrapping_add(1);
        Ok(true)
    }

    /// Makes every pushed entry visible. Returns true if the consumer is
    /// idle and needs a wake-up.
    pub fn publish(&mut self) -> bool {
        let r = &self.ends.region;
        r.u32_at(self.ends.own).store(self.tail, Ordering::Release);
        // Pairs with the fence in `prepare_wait` so a wake-up is never lost.
        fence(Ordering::SeqCst);
        r.u32_at(self.ends.idle).load(Ordering::Relaxed) == 1
    }

    /// Entries pushed and not yet released by the consumer, as last seen.
    pub fn pending(&self) -> u32 {
        self.tail.wrapping_sub(self.head)
    }
}

/// The consuming end of one ring. Make at most one per ring.
#[derive(Debug)]
pub struct Consumer<const N: usize> {
    ends: Ends,
    head: u32,
    tail: u32,
}

impl<const N: usize> Consumer<N> {
    fn new(ends: Ends) -> Self {
        Self {
            ends,
            head: 0,
            tail: 0,
        }
    }

    /// Copies out the next entry, if there is one. The slot stays the
    /// consumer's until [`Consumer::release`].
    pub fn pop(&mut self) -> Result<Option<[u8; N]>, Broken> {
        if self.head == self.tail {
            let t = self
                .ends
                .region
                .u32_at(self.ends.peer)
                .load(Ordering::Acquire);
            self.see_tail(t)?;
            if self.head == self.tail {
                return Ok(None);
            }
        }
        let mut e = [0u8; N];
        self.ends
            .region
            .load(self.ends.slot::<N>(self.head), &mut e);
        self.head = self.head.wrapping_add(1);
        Ok(Some(e))
    }

    /// The tail as last loaded. After a [`Consumer::pop`] that returned
    /// `None`, every entry up to it is taken.
    pub fn seen_tail(&self) -> u32 {
        self.tail
    }

    /// Loads and checks the tail without taking anything. The engine uses
    /// it to see entries published while the queue is paused.
    pub fn load_tail(&mut self) -> Result<u32, Broken> {
        let t = self
            .ends
            .region
            .u32_at(self.ends.peer)
            .load(Ordering::Acquire);
        self.see_tail(t)?;
        Ok(t)
    }

    /// Hands every popped slot back to the producer.
    pub fn release(&mut self) {
        self.ends
            .region
            .u32_at(self.ends.own)
            .store(self.head, Ordering::Release);
    }

    /// Sets the idle flag and checks the ring again. Returns true if it is
    /// still empty and the caller may wait; it must then call
    /// [`Consumer::woke`] after the wake-up read.
    pub fn prepare_wait(&mut self) -> Result<bool, Broken> {
        let region = Arc::clone(&self.ends.region);
        let idle = region.u32_at(self.ends.idle);
        idle.store(1, Ordering::Relaxed);
        fence(Ordering::SeqCst);
        let t = region.u32_at(self.ends.peer).load(Ordering::Acquire);
        self.see_tail(t)?;
        if self.head != self.tail {
            idle.store(0, Ordering::Relaxed);
            return Ok(false);
        }
        Ok(true)
    }

    /// Clears the idle flag.
    pub fn woke(&mut self) {
        self.ends
            .region
            .u32_at(self.ends.idle)
            .store(0, Ordering::Relaxed);
    }

    /// A valid tail is at most `depth` past our head and not behind the
    /// last tail seen.
    fn see_tail(&mut self, t: u32) -> Result<(), Broken> {
        let ahead = t.wrapping_sub(self.head);
        if ahead > self.ends.depth || ahead < self.tail.wrapping_sub(self.head) {
            return Err(Broken);
        }
        self.tail = t;
        Ok(())
    }
}
