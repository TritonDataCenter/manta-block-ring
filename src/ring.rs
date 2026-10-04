//! Moving entries and data through the shared region.
//!
//! The peer can write any byte at any time, so every access here is an
//! atomic or a bulk copy (below), never a plain reference or
//! `copy_nonoverlapping`. A torn entry is then only bad input for the checks. Each side keeps its own index in
//! private memory and never reads its own field back.

#![allow(unsafe_code)]

use std::any::Any;
use std::fmt;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering, fence};

use crate::error::{Broken, LayoutError};
use crate::layout::{CQE_BYTES, Geometry, HEADER_BYTES, Header, PAGE, SQE_BYTES, control};

/// A mapped region. All access goes through atomics or bulk copies.
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

// SAFETY: only atomic or `bulk` access, to memory that stays mapped while
// the Region lives.
unsafe impl Send for Region {}
// SAFETY: as for Send; shared references only allow atomic or `bulk` access.
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
        let (chunks, tail) = out.as_chunks_mut::<8>();
        debug_assert!(tail.is_empty());
        for (i, chunk) in chunks.iter_mut().enumerate() {
            *chunk = self
                .u64_at(off + i * 8)
                .load(Ordering::Relaxed)
                .to_ne_bytes();
        }
    }

    /// `data.len()` and `off` must be multiples of 8 and inside the region.
    fn store(&self, off: usize, data: &[u8]) {
        let (chunks, tail) = data.as_chunks::<8>();
        debug_assert!(tail.is_empty());
        for (i, chunk) in chunks.iter().enumerate() {
            self.u64_at(off + i * 8)
                .store(u64::from_ne_bytes(*chunk), Ordering::Relaxed);
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
                self.load_bulk(off, out);
                true
            }
            None => false,
        }
    }

    /// Same rules as [`Region::read_buffer`].
    pub fn write_buffer(&self, q: u16, page: u32, data: &[u8]) -> bool {
        match self.buffer_range(q, page, data.len()) {
            Some(off) => {
                self.store_bulk(off, data);
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

#[cfg(all(not(miri), any(target_arch = "x86_64", target_arch = "aarch64")))]
impl Region {
    /// Copies the buffer area out. The buffer area is only ever accessed
    /// this way, so access sizes in this process never mix.
    fn load_bulk(&self, off: usize, out: &mut [u8]) {
        debug_assert!(off + out.len() <= self.geometry.region_len());
        // SAFETY: the range is inside the mapping, which outlives `&self`.
        // `out` is ours and cannot overlap it.
        unsafe { bulk::copy(self.base.as_ptr().add(off), out.as_mut_ptr(), out.len()) }
    }

    fn store_bulk(&self, off: usize, data: &[u8]) {
        debug_assert!(off + data.len() <= self.geometry.region_len());
        // SAFETY: as in `load_bulk`.
        unsafe { bulk::copy(data.as_ptr(), self.base.as_ptr().add(off), data.len()) }
    }
}

// Miri cannot run asm, and other targets have no asm copy yet.
#[cfg(not(all(not(miri), any(target_arch = "x86_64", target_arch = "aarch64"))))]
impl Region {
    fn load_bulk(&self, off: usize, out: &mut [u8]) {
        self.load(off, out);
    }

    fn store_bulk(&self, off: usize, data: &[u8]) {
        self.store(off, data);
    }
}

/// A copy that a concurrent writer cannot make undefined.
///
/// A plain or volatile copy that races with a write is a data race, which is
/// undefined behavior even if the bytes are never trusted. Inline asm is
/// opaque to the Rust memory model: what it does to memory counts as some
/// Rust code that could do the same, and these instructions do no more than
/// a relaxed `AtomicU8` load and store per byte. A race then only tears the
/// data, which the callers already treat as untrusted. The asm is not
/// `readonly` or `nomem`, so the compiler cannot move memory access across
/// it, and the `Acquire` and `Release` on the ring indices still order it.
#[cfg(all(not(miri), any(target_arch = "x86_64", target_arch = "aarch64")))]
mod bulk {
    use std::arch::asm;

    /// # Safety
    ///
    /// `src` is readable and `dst` writable for `len` bytes, and the ranges
    /// do not overlap. Another process may write either range at any time;
    /// another thread in this process only with this copy.
    #[cfg(target_arch = "x86_64")]
    pub(super) unsafe fn copy(src: *const u8, dst: *mut u8, len: usize) {
        // ERMS and FSRM make `rep movsb` as fast as memcpy on the CPUs we run
        // on. The ABI guarantees the direction flag is clear.
        // SAFETY: the caller's guarantee; see the module comment.
        unsafe {
            asm!(
                "rep movsb",
                inout("rcx") len => _,
                inout("rsi") src => _,
                inout("rdi") dst => _,
                options(nostack, preserves_flags),
            );
        }
    }

    /// # Safety
    ///
    /// As for the x86_64 version, and `len` is a multiple of 8.
    #[cfg(target_arch = "aarch64")]
    pub(super) unsafe fn copy(src: *const u8, dst: *mut u8, len: usize) {
        debug_assert!(len.is_multiple_of(8));
        // SAFETY: the caller's guarantee; see the module comment.
        unsafe {
            asm!(
                "tbz {n}, #3, 2f",
                "ldr {a}, [{src}], #8",
                "str {a}, [{dst}], #8",
                "sub {n}, {n}, #8",
                "2:",
                "cbz {n}, 3f",
                "1:",
                "ldp {a}, {b}, [{src}], #16",
                "stp {a}, {b}, [{dst}], #16",
                "subs {n}, {n}, #16",
                "b.ne 1b",
                "3:",
                n = inout(reg) len => _,
                src = inout(reg) src => _,
                dst = inout(reg) dst => _,
                a = out(reg) _,
                b = out(reg) _,
                options(nostack),
            );
        }
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

#[cfg(test)]
mod tests {
    use std::alloc::{Layout, alloc_zeroed};
    use std::hint::black_box;
    use std::time::Instant;

    use super::*;

    /// Never freed, so it outlives the Region.
    fn region(buf_pages: u32) -> Region {
        let g = Geometry::new(1, 2, buf_pages).unwrap();
        let layout = Layout::from_size_align(g.region_len(), PAGE).unwrap();
        // SAFETY: the layout has a non-zero size.
        let base = unsafe { alloc_zeroed(layout) };
        assert!(!base.is_null());
        // SAFETY: a fresh, never-freed allocation only used through the
        // Region.
        unsafe { Region::from_raw_parts(base, g.region_len(), g) }.unwrap()
    }

    fn pattern(len: usize, seed: u64) -> Vec<u8> {
        let mut x = seed | 1;
        (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect()
    }

    #[test]
    fn bulk_copies_match_the_atomic_path() {
        let r = region(4);
        let end = r.geometry().region_len();
        let lens: &[usize] = if cfg!(miri) {
            &[0, 8, 24, 4096]
        } else {
            &[
                0, 8, 16, 24, 56, 64, 72, 504, 4088, 4096, 4104, 8192, 12_288,
            ]
        };
        let offs: &[usize] = if cfg!(miri) {
            &[0, 8, 4096 + 40]
        } else {
            &[0, 8, 16, 40, 4096 - 8, 4096, 4096 + 8, 2 * 4096 + 24]
        };
        for &len in lens {
            for &off in offs {
                let off = r.geometry().buf_offset(0) + off;
                if off + len > end {
                    continue;
                }
                let data = pattern(len, (len ^ off) as u64);
                r.store(off, &data);
                let (mut old, mut new) = (vec![0u8; len], vec![0u8; len]);
                r.load(off, &mut old);
                r.load_bulk(off, &mut new);
                assert_eq!(old, data, "len {len} off {off}");
                assert_eq!(new, data, "len {len} off {off}");

                let data = pattern(len, !(len ^ off) as u64);
                r.store_bulk(off, &data);
                r.load(off, &mut old);
                assert_eq!(old, data, "len {len} off {off}");
            }
        }
    }

    /// The asm copy itself takes any alignment.
    #[cfg(all(not(miri), any(target_arch = "x86_64", target_arch = "aarch64")))]
    #[test]
    fn bulk_copy_handles_any_alignment() {
        let src = pattern(1024 + 16, 7);
        for len in (0..=1024).step_by(8) {
            for s in 0..16 {
                for d in 0..16 {
                    let mut dst = vec![0u8; 1024 + 32];
                    // SAFETY: both ranges are in bounds of separate vectors.
                    unsafe { bulk::copy(src.as_ptr().add(s), dst.as_mut_ptr().add(d), len) };
                    assert_eq!(&dst[d..d + len], &src[s..s + len]);
                    assert!(dst[..d].iter().all(|&b| b == 0));
                    assert!(dst[d + len..].iter().all(|&b| b == 0));
                }
            }
        }
    }

    /// `cargo test --release --lib -- --ignored --nocapture bulk_copy_speed`
    #[test]
    #[ignore = "benchmark"]
    fn bulk_copy_speed() {
        let r = region(32);
        let off = r.geometry().buf_offset(0);
        for len in [4096, 16 << 10, 64 << 10, 128 << 10] {
            let out = vec![0u8; len];
            let iters = (256 << 20) / len;
            let time = |f: &dyn Fn(&mut [u8])| {
                let mut out = out.clone();
                f(&mut out);
                let t = Instant::now();
                for _ in 0..iters {
                    f(black_box(&mut out));
                }
                t.elapsed().as_nanos() as f64 / iters as f64
            };
            let old_read = time(&|o| r.load(off, o));
            let new_read = time(&|o| r.load_bulk(off, o));
            let old_write = time(&|o| r.store(off, o));
            let new_write = time(&|o| r.store_bulk(off, o));
            let mut plain = vec![0u8; len];
            let t = Instant::now();
            for _ in 0..iters {
                black_box(&mut plain).copy_from_slice(black_box(&out));
            }
            let memcpy = t.elapsed().as_nanos() as f64 / iters as f64;
            println!(
                "{:>4} KiB  read old {old_read:>8.0} new {new_read:>8.0}  \
                 write old {old_write:>8.0} new {new_write:>8.0}  memcpy {memcpy:>8.0} ns",
                len >> 10
            );
        }
    }
}
