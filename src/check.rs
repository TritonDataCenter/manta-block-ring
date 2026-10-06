//! The checks each side runs on what the other side sends.

use crate::entry::{Cqe, Op, OpId, Sqe};
use crate::error::{BadCompletion, Reject};
use crate::layout::PAGE;

/// What the engine agreed for one attachment, in private memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Issued from FoundationDB at attach.
    pub generation: u64,
    /// Entries per ring.
    pub depth: u32,
    /// Buffer pages per queue.
    pub buf_pages: u32,
    /// Volume size in 4 KiB blocks.
    pub volume_blocks: u64,
    /// Most blocks one read or write may carry.
    pub max_blocks: u32,
    /// The volume's logical sector: 4096, or 512 once Hello agreed
    /// `sector-512e`. Below 4096, a write, deallocate or write zeroes may
    /// name a byte range inside its blocks.
    pub sector_bytes: u32,
    /// Ops 4 and 5 are allowed: Hello agreed `deallocate` and the volume
    /// takes them.
    pub zeroing: bool,
}

/// A request that passed every check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Request {
    /// rust-bhyve's slot, echoed in the completion.
    pub tag: u16,
    /// Read, write or flush.
    pub op: Op,
    /// Decoded from the entry.
    pub op_id: OpId,
    /// First 4 KiB block (0 for flush).
    pub lba: u64,
    /// 4 KiB blocks (0 for flush).
    pub blocks: u32,
    /// First buffer page (0 for flush, deallocate and write zeroes).
    pub buf_page: u32,
    /// Where the guest range starts in the first block, in bytes.
    pub byte_off: u16,
    /// Bytes of the guest range; 0 when it is every byte of `blocks`.
    pub byte_len: u32,
}

impl Request {
    /// True when the guest range does not cover whole blocks.
    pub fn is_partial(&self) -> bool {
        self.byte_len != 0
    }

    /// The guest range as byte offsets inside the request's blocks.
    pub fn guest_bytes(&self) -> std::ops::Range<u64> {
        if self.byte_len == 0 {
            return 0..u64::from(self.blocks) * PAGE as u64;
        }
        let from = u64::from(self.byte_off);
        from..from + u64::from(self.byte_len)
    }
}

/// The engine's state for one queue of one attachment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueState {
    queue: u16,
    limits: Limits,
    tags: Vec<u64>,
    in_flight: u32,
    last_seq: Option<u64>,
    floor: u64,
}

impl QueueState {
    /// `floor` is the highest watermark already known for this generation
    /// and queue, or 0.
    pub fn new(queue: u16, limits: Limits, floor: u64) -> Self {
        Self {
            queue,
            limits,
            tags: vec![0; (limits.depth as usize).div_ceil(64)],
            in_flight: 0,
            last_seq: None,
            floor,
        }
    }

    /// Fewer than `depth` in flight, so every completion fits in the CQ.
    pub fn can_take(&self) -> bool {
        self.in_flight < self.limits.depth
    }

    /// Requests taken and not yet completed.
    pub fn in_flight(&self) -> u32 {
        self.in_flight
    }

    /// No request below this sequence is taken.
    pub fn floor(&self) -> u64 {
        self.floor
    }

    /// On success the request is in flight until [`QueueState::complete`].
    /// Call only when [`QueueState::can_take`] is true.
    pub fn check(&mut self, e: &Sqe) -> Result<Request, Reject> {
        let op = Op::from_u8(e.op).ok_or(Reject::Op(e.op))?;
        if e.flags != 0 || e.reserved != [0; 6] {
            return Err(Reject::Reserved);
        }
        if u32::from(e.tag) >= self.limits.depth {
            return Err(Reject::Tag(e.tag));
        }
        if self.tag_in_flight(e.tag) {
            return Err(Reject::TagInFlight(e.tag));
        }
        let id = OpId::from_u128(e.op_id);
        if id.generation != self.limits.generation {
            return Err(Reject::Generation(id.generation));
        }
        if id.queue != self.queue {
            return Err(Reject::Queue(id.queue));
        }
        if let Some(last) = self.last_seq
            && id.seq <= last
        {
            return Err(Reject::SeqNotIncreasing {
                last,
                found: id.seq,
            });
        }
        if id.seq < self.floor {
            return Err(Reject::BelowFloor {
                floor: self.floor,
                found: id.seq,
            });
        }
        if e.watermark > id.seq {
            return Err(Reject::Watermark);
        }
        if e.byte_off != 0 && e.byte_len == 0 {
            return Err(Reject::Partial);
        }
        match op {
            Op::Flush => {
                if e.lba != 0 || e.blocks != 0 || e.buf_page != 0 || e.byte_len != 0 {
                    return Err(Reject::Flush);
                }
            }
            Op::Read | Op::Write => {
                if e.blocks == 0 || e.blocks > self.limits.max_blocks {
                    return Err(Reject::Blocks(e.blocks));
                }
                self.check_range(e)?;
                let buf_end = u64::from(e.buf_page) + u64::from(e.blocks);
                if buf_end > u64::from(self.limits.buf_pages) {
                    return Err(Reject::Buffer);
                }
                if op == Op::Read && e.byte_len != 0 {
                    return Err(Reject::Partial);
                }
                self.check_partial(e)?;
            }
            Op::Deallocate | Op::WriteZeroes => {
                if !self.limits.zeroing {
                    return Err(Reject::Op(e.op));
                }
                // A range is merged with data, so it is bound as a write is.
                if e.blocks == 0 || (e.byte_len != 0 && e.blocks > self.limits.max_blocks) {
                    return Err(Reject::Blocks(e.blocks));
                }
                self.check_range(e)?;
                if e.buf_page != 0 {
                    return Err(Reject::Buffer);
                }
                self.check_partial(e)?;
            }
        }
        self.set_tag(e.tag, true);
        self.in_flight += 1;
        self.last_seq = Some(id.seq);
        self.floor = self.floor.max(e.watermark);
        Ok(Request {
            tag: e.tag,
            op,
            op_id: id,
            lba: e.lba,
            blocks: e.blocks,
            buf_page: e.buf_page,
            byte_off: e.byte_off,
            byte_len: e.byte_len,
        })
    }

    fn check_range(&self, e: &Sqe) -> Result<(), Reject> {
        let end = e.lba.checked_add(u64::from(e.blocks));
        if end.is_none_or(|end| end > self.limits.volume_blocks) {
            return Err(Reject::Range);
        }
        Ok(())
    }

    /// A byte range must name whole sectors, start in the first block,
    /// end in the last, and leave out at least one sector, so each
    /// request has one encoding and a replay can be matched exactly.
    fn check_partial(&self, e: &Sqe) -> Result<(), Reject> {
        if e.byte_len == 0 {
            return Ok(());
        }
        let sector = u64::from(self.limits.sector_bytes);
        let (off, len) = (u64::from(e.byte_off), u64::from(e.byte_len));
        let span = u64::from(e.blocks) * PAGE as u64;
        let end = off + len;
        let fine = sector != 0
            && sector < PAGE as u64
            && off.is_multiple_of(sector)
            && len.is_multiple_of(sector)
            && off < PAGE as u64
            && end <= span
            && end > span - PAGE as u64
            && (off != 0 || end != span);
        if fine { Ok(()) } else { Err(Reject::Partial) }
    }

    /// Call once the completion is in the ring. Returns false if nothing
    /// with that tag was in flight.
    pub fn complete(&mut self, tag: u16) -> bool {
        if u32::from(tag) >= self.limits.depth || !self.tag_in_flight(tag) {
            return false;
        }
        self.set_tag(tag, false);
        self.in_flight -= 1;
        true
    }

    fn tag_in_flight(&self, tag: u16) -> bool {
        let t = usize::from(tag);
        self.tags[t / 64] & (1 << (t % 64)) != 0
    }

    fn set_tag(&mut self, tag: u16, on: bool) {
        let t = usize::from(tag);
        if on {
            self.tags[t / 64] |= 1 << (t % 64);
        } else {
            self.tags[t / 64] &= !(1 << (t % 64));
        }
    }
}

/// rust-bhyve's requests in flight on one queue, by tag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outstanding {
    slots: Vec<Option<u64>>,
}

impl Outstanding {
    /// An empty table with `depth` tags.
    pub fn new(depth: u32) -> Self {
        Self {
            slots: vec![None; depth as usize],
        }
    }

    /// A free tag, if any.
    pub fn free_tag(&self) -> Option<u16> {
        self.slots
            .iter()
            .position(Option::is_none)
            .map(|t| t as u16)
    }

    /// Returns false if the tag is out of range or already in use.
    pub fn insert(&mut self, tag: u16, op_id: OpId) -> bool {
        match self.slots.get_mut(usize::from(tag)) {
            Some(slot @ None) => {
                *slot = Some(op_id.low());
                true
            }
            _ => false,
        }
    }

    /// Frees the tag if the completion matches.
    pub fn complete(&mut self, c: &Cqe) -> Result<(), BadCompletion> {
        let slot = self
            .slots
            .get_mut(usize::from(c.tag))
            .ok_or(BadCompletion::Tag(c.tag))?;
        match *slot {
            None => Err(BadCompletion::Tag(c.tag)),
            Some(seq) if seq != c.op_seq => Err(BadCompletion::OpSeq),
            Some(_) => {
                *slot = None;
                Ok(())
            }
        }
    }

    /// Requests still waiting for a completion.
    pub fn len(&self) -> usize {
        self.slots.iter().filter(|s| s.is_some()).count()
    }

    /// True when nothing is in flight.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::Status;

    const GEN: u64 = 9;

    fn limits() -> Limits {
        Limits {
            generation: GEN,
            depth: 8,
            buf_pages: 16,
            volume_blocks: 1000,
            max_blocks: 8,
            sector_bytes: 4096,
            zeroing: false,
        }
    }

    fn sqe(tag: u16, seq: u64, op: u8, lba: u64, blocks: u32, buf_page: u32) -> Sqe {
        Sqe {
            op_id: OpId {
                generation: GEN,
                queue: 2,
                seq,
            }
            .to_u128(),
            lba,
            blocks,
            buf_page,
            watermark: 0,
            queued_at: 0,
            tag,
            op,
            flags: 0,
            byte_off: 0,
            byte_len: 0,
            reserved: [0; 6],
        }
    }

    #[test]
    fn accepts_and_tracks_in_flight() {
        let mut q = QueueState::new(2, limits(), 0);
        let r = q.check(&sqe(1, 0, 2, 10, 4, 0)).unwrap();
        assert_eq!((r.op, r.lba, r.blocks), (Op::Write, 10, 4));
        assert_eq!(q.in_flight(), 1);
        assert_eq!(q.check(&sqe(1, 1, 1, 0, 1, 0)), Err(Reject::TagInFlight(1)));
        assert!(q.complete(1));
        assert!(!q.complete(1));
        assert!(q.check(&sqe(1, 1, 1, 0, 1, 0)).is_ok());
    }

    #[test]
    fn refuses_each_broken_rule() {
        let mut q = QueueState::new(2, limits(), 0);
        assert_eq!(q.check(&sqe(0, 0, 4, 0, 1, 0)), Err(Reject::Op(4)));
        let mut e = sqe(0, 0, 1, 0, 1, 0);
        e.reserved[5] = 1;
        assert_eq!(q.check(&e), Err(Reject::Reserved));
        assert_eq!(q.check(&sqe(8, 0, 1, 0, 1, 0)), Err(Reject::Tag(8)));
        let mut e = sqe(0, 0, 1, 0, 1, 0);
        e.op_id = OpId {
            generation: GEN + 1,
            queue: 2,
            seq: 0,
        }
        .to_u128();
        assert_eq!(q.check(&e), Err(Reject::Generation(GEN + 1)));
        e.op_id = OpId {
            generation: GEN,
            queue: 3,
            seq: 0,
        }
        .to_u128();
        assert_eq!(q.check(&e), Err(Reject::Queue(3)));
        assert_eq!(q.check(&sqe(0, 0, 1, 0, 9, 0)), Err(Reject::Blocks(9)));
        assert_eq!(q.check(&sqe(0, 0, 1, 0, 0, 0)), Err(Reject::Blocks(0)));
        assert_eq!(q.check(&sqe(0, 0, 1, 996, 5, 0)), Err(Reject::Range));
        assert_eq!(q.check(&sqe(0, 0, 1, u64::MAX, 1, 0)), Err(Reject::Range));
        assert_eq!(q.check(&sqe(0, 0, 1, 0, 4, 13)), Err(Reject::Buffer));
        assert_eq!(q.check(&sqe(0, 0, 1, 0, 1, u32::MAX)), Err(Reject::Buffer));
        assert_eq!(q.check(&sqe(0, 0, 3, 0, 1, 0)), Err(Reject::Flush));
        let mut e = sqe(0, 5, 1, 0, 1, 0);
        e.watermark = 6;
        assert_eq!(q.check(&e), Err(Reject::Watermark));
        // Nothing above was taken.
        assert_eq!(q.in_flight(), 0);
    }

    fn partial(off: u16, len: u32, lba: u64, blocks: u32, op: u8) -> Sqe {
        let mut e = sqe(0, 0, op, lba, blocks, 0);
        e.byte_off = off;
        e.byte_len = len;
        e
    }

    #[test]
    fn a_byte_range_needs_512e_and_whole_sectors_inside_its_blocks() {
        let mut q = QueueState::new(2, limits(), 0);
        assert_eq!(q.check(&partial(512, 512, 0, 1, 2)), Err(Reject::Partial));
        let l = Limits {
            sector_bytes: 512,
            ..limits()
        };
        let bad = [
            (512, 0, 1),     // an offset with no length
            (100, 512, 1),   // not a whole sector
            (512, 100, 1),   // not a whole sector
            (0, 4096, 1),    // the whole block: send it with no range
            (0, 8192, 2),    // the whole blocks
            (3584, 1024, 1), // past the block
            (4096, 512, 2),  // starts past the first block
            (0, 512, 2),     // ends before the last block
            (512, 4096, 3),  // ends before the last block
        ];
        for (off, len, blocks) in bad {
            let mut q = QueueState::new(2, l, 0);
            assert_eq!(
                q.check(&partial(off, len, 0, blocks, 2)),
                Err(Reject::Partial),
                "{off} {len} {blocks}"
            );
        }
        for (off, len, blocks) in [(512, 512, 1), (0, 3584, 1), (3584, 1024, 2), (512, 8192, 3)] {
            let mut q = QueueState::new(2, l, 0);
            let r = q.check(&partial(off, len, 0, blocks, 2)).unwrap();
            assert!(r.is_partial());
            assert_eq!(
                r.guest_bytes(),
                u64::from(off)..u64::from(off) + u64::from(len)
            );
        }
        let mut q = QueueState::new(2, l, 0);
        assert_eq!(q.check(&partial(512, 512, 0, 1, 1)), Err(Reject::Partial));
        assert_eq!(q.check(&partial(0, 512, 0, 0, 3)), Err(Reject::Flush));
    }

    #[test]
    fn zeroing_ops_need_the_feature_and_carry_no_buffer() {
        let mut q = QueueState::new(2, limits(), 0);
        assert_eq!(q.check(&sqe(0, 0, 4, 0, 1, 0)), Err(Reject::Op(4)));
        assert_eq!(q.check(&sqe(0, 0, 5, 0, 1, 0)), Err(Reject::Op(5)));
        let l = Limits {
            zeroing: true,
            ..limits()
        };
        let mut q = QueueState::new(2, l, 0);
        assert_eq!(q.check(&sqe(0, 0, 4, 0, 1, 1)), Err(Reject::Buffer));
        assert_eq!(q.check(&sqe(0, 0, 4, 0, 0, 0)), Err(Reject::Blocks(0)));
        assert_eq!(q.check(&sqe(0, 0, 5, 999, 2, 0)), Err(Reject::Range));
        // No data, so not bound by max_blocks.
        let r = q.check(&sqe(0, 0, 4, 0, 1000, 0)).unwrap();
        assert_eq!((r.op, r.blocks), (Op::Deallocate, 1000));
        let r = q.check(&sqe(1, 1, 5, 7, 1, 0)).unwrap();
        assert_eq!(r.op, Op::WriteZeroes);
        let l512 = Limits {
            sector_bytes: 512,
            ..l
        };
        let mut q = QueueState::new(2, l512, 0);
        assert_eq!(
            q.check(&partial(512, 9 * 4096 - 1024, 0, 9, 4)),
            Err(Reject::Blocks(9))
        );
        assert!(q.check(&partial(512, 512, 0, 1, 4)).is_ok());
        // A 4 KiB volume zeroes whole blocks only.
        let mut q = QueueState::new(2, l, 0);
        assert_eq!(q.check(&partial(512, 512, 0, 1, 5)), Err(Reject::Partial));
    }

    #[test]
    fn sequences_go_up_and_the_floor_holds() {
        let mut q = QueueState::new(2, limits(), 3);
        assert_eq!(
            q.check(&sqe(0, 2, 1, 0, 1, 0)),
            Err(Reject::BelowFloor { floor: 3, found: 2 })
        );
        let mut e = sqe(0, 5, 1, 0, 1, 0);
        e.watermark = 5;
        q.check(&e).unwrap();
        assert_eq!(q.floor(), 5);
        assert_eq!(
            q.check(&sqe(1, 5, 1, 0, 1, 0)),
            Err(Reject::SeqNotIncreasing { last: 5, found: 5 })
        );
    }

    #[test]
    fn depth_bounds_in_flight() {
        let mut q = QueueState::new(2, limits(), 0);
        for t in 0..8u16 {
            assert!(q.can_take());
            q.check(&sqe(t, u64::from(t), 3, 0, 0, 0)).unwrap();
        }
        assert!(!q.can_take());
    }

    #[test]
    fn outstanding_matches_completions() {
        let mut o = Outstanding::new(4);
        let id = OpId {
            generation: GEN,
            queue: 0,
            seq: 7,
        };
        assert!(o.insert(2, id));
        assert!(!o.insert(2, id));
        assert!(!o.insert(4, id));
        let mut c = Cqe {
            tag: 2,
            status: Status::Success,
            op_seq: id.low() + 1,
            engine_ns: 0,
            durable_ns: 0,
        };
        assert_eq!(o.complete(&c), Err(BadCompletion::OpSeq));
        c.op_seq = id.low();
        assert_eq!(o.complete(&c), Ok(()));
        assert_eq!(o.complete(&c), Err(BadCompletion::Tag(2)));
        assert!(o.is_empty());
    }
}
