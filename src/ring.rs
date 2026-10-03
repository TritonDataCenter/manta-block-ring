//! Moving entries and data through the shared region (spec 37 §4).
//!
//! The other side can write any byte of the region at any time. So every
//! access to shared memory here is an atomic load or store, never a plain
//! reference or `copy_nonoverlapping`: that keeps this process sound even
//! when the peer writes while we read. A torn entry is then only bad input,
//! which the checks refuse.
//!
//! Each side keeps its own index in private memory and only writes it out;
//! it never reads its own field back, because the peer can rewrite it.

#![allow(unsafe_code)]

use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering, fence};

use crate::error::{Broken, LayoutError};
use crate::layout::{CQE_BYTES, Geometry, HEADER_BYTES, Header, PAGE, SQE_BYTES, control};

/// A mapped region. All access goes through atomics.
#[derive(Debug)]
pub struct Region {
    base: NonNull<u8>,
    geometry: Geometry,
}

// SAFETY: a Region only hands out atomic accesses to memory that, by the
// contract of `from_raw_parts`, stays mapped while the Region lives. Atomics
// are safe to use from any thread.
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
        Ok(Self { base, geometry })
    }

    /// The geometry the region was made with.
    pub fn geometry(&self) -> &Geometry {
        &self.geometry
    }

    fn u32_at(&self, off: usize) -> &AtomicU32 {
        debug_assert!(off.is_multiple_of(4) && off + 4 <= self.geometry.region_len());
        // SAFETY: every caller computes `off` from the checked geometry, so it
        // is inside the mapping and 4-byte aligned (the base is page-aligned).
        // The mapping outlives `&self`, and this process only uses atomics on
        // it.
        unsafe { AtomicU32::from_ptr(self.base.as_ptr().add(off).cast()) }
    }

    fn u64_at(&self, off: usize) -> &AtomicU64 {
        debug_assert!(off.is_multiple_of(8) && off + 8 <= self.geometry.region_len());
        // SAFETY: as in `u32_at`, with 8-byte alignment.
        unsafe { AtomicU64::from_ptr(self.base.as_ptr().add(off).cast()) }
    }

    /// Copies bytes out of the region, 8 at a time. `out.len()` and `off`
    /// are multiples of 8, and the range is inside the region.
    fn load(&self, off: usize, out: &mut [u8]) {
        for (i, chunk) in out.chunks_exact_mut(8).enumerate() {
            let v = self.u64_at(off + i * 8).load(Ordering::Relaxed);
            chunk.copy_from_slice(&v.to_ne_bytes());
        }
    }

    /// Copies bytes into the region, 8 at a time. Same rules as `load`.
    fn store(&self, off: usize, data: &[u8]) {
        for (i, chunk) in data.chunks_exact(8).enumerate() {
            let mut v = [0u8; 8];
            v.copy_from_slice(chunk);
            self.u64_at(off + i * 8)
                .store(u64::from_ne_bytes(v), Ordering::Relaxed);
        }
    }

    /// Writes the region header. The engine does this once, before it sends
    /// the region, and never reads it back.
    pub fn write_header(&self, h: &Header) {
        self.store(0, &h.encode());
    }

    /// Reads and checks the header. rust-bhyve does this once at attach.
    pub fn read_header(&self) -> Result<Header, LayoutError> {
        let mut b = [0u8; HEADER_BYTES];
        self.load(0, &mut b);
        Header::decode(&b)
    }

    /// Copies `out.len()` bytes from queue `q`'s buffer area, starting at
    /// page `page`. Returns false if the range is outside the area or the
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

    /// Copies `data` into queue `q`'s buffer area at page `page`. Same rules
    /// as [`Region::read_buffer`].
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
    /// waiting and needs a wake-up (spec 37 §4, step 4).
    pub fn publish(&mut self) -> bool {
        let r = &self.ends.region;
        r.u32_at(self.ends.own).store(self.tail, Ordering::Release);
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

    /// Hands every popped slot back to the producer.
    pub fn release(&mut self) {
        self.ends
            .region
            .u32_at(self.ends.own)
            .store(self.head, Ordering::Release);
    }

    /// Steps 1 and 2 of the wake-up state machine (spec 37 §4). Returns true
    /// if the ring is still empty and the caller may wait for a wake-up;
    /// it must then call [`Consumer::woke`] after the wake-up read.
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

    /// Step 3, after the wake-up read: not waiting any more.
    pub fn woke(&mut self) {
        self.ends
            .region
            .u32_at(self.ends.idle)
            .store(0, Ordering::Relaxed);
    }

    /// A tail is valid if it is at most `depth` entries past our head and
    /// not behind the tail we saw last.
    fn see_tail(&mut self, t: u32) -> Result<(), Broken> {
        let ahead = t.wrapping_sub(self.head);
        if ahead > self.ends.depth || ahead < self.tail.wrapping_sub(self.head) {
            return Err(Broken);
        }
        self.tail = t;
        Ok(())
    }
}
