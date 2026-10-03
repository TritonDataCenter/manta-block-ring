//! The checks each side runs on what the other side sends (spec 37 §4).
//!
//! [`QueueState`] is the engine's view of one queue. It never trusts a
//! value from shared memory: it checks every request against what it agreed
//! at attach and what it has already taken. [`Outstanding`] is rust-bhyve's
//! table of requests in flight, which every completion must match.

use crate::entry::{Cqe, Op, OpId, Sqe};
use crate::error::{BadCompletion, Reject};

/// What the engine agreed for one attachment, in private memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// The attachment's generation, from FoundationDB.
    pub generation: u64,
    /// Entries per ring.
    pub depth: u32,
    /// Buffer pages per queue.
    pub buf_pages: u32,
    /// Volume size in 4 KiB blocks.
    pub volume_blocks: u64,
    /// Most blocks one request may carry.
    pub max_blocks: u32,
}

/// A request that passed every check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Request {
    /// rust-bhyve's slot, echoed in the completion.
    pub tag: u16,
    /// Read, write or flush.
    pub op: Op,
    /// The request id.
    pub op_id: OpId,
    /// First 4 KiB block (0 for flush).
    pub lba: u64,
    /// 4 KiB blocks (0 for flush).
    pub blocks: u32,
    /// First buffer page (0 for flush).
    pub buf_page: u32,
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
    /// A queue with nothing taken yet. `floor` is the highest watermark
    /// already known for this generation and queue (0 if none).
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

    /// True while the engine may take another request: fewer than `depth`
    /// are in flight, so their completions always fit in the ring.
    pub fn can_take(&self) -> bool {
        self.in_flight < self.limits.depth
    }

    /// Requests taken and not yet completed.
    pub fn in_flight(&self) -> u32 {
        self.in_flight
    }

    /// The highest watermark seen: no request below it is taken.
    pub fn floor(&self) -> u64 {
        self.floor
    }

    /// Checks a request copied out of the ring. On success the request is
    /// in flight until [`QueueState::complete`]. Call only when
    /// [`QueueState::can_take`] is true.
    pub fn check(&mut self, e: &Sqe) -> Result<Request, Reject> {
        let op = Op::from_u8(e.op).ok_or(Reject::Op(e.op))?;
        if e.flags != 0 || e.reserved != [0; 12] {
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
        match op {
            Op::Flush => {
                if e.lba != 0 || e.blocks != 0 || e.buf_page != 0 {
                    return Err(Reject::Flush);
                }
            }
            Op::Read | Op::Write => {
                if e.blocks == 0 || e.blocks > self.limits.max_blocks {
                    return Err(Reject::Blocks(e.blocks));
                }
                let end = e.lba.checked_add(u64::from(e.blocks));
                if end.is_none_or(|end| end > self.limits.volume_blocks) {
                    return Err(Reject::Range);
                }
                let buf_end = u64::from(e.buf_page) + u64::from(e.blocks);
                if buf_end > u64::from(self.limits.buf_pages) {
                    return Err(Reject::Buffer);
                }
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
        })
    }

    /// Marks a taken request complete once its completion is in the ring.
    /// Returns false if nothing with that tag was in flight.
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
    /// An empty table for a ring of `depth` entries.
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

    /// Records a request sent with `tag`. Returns false if the tag is out
    /// of range or already in use.
    pub fn insert(&mut self, tag: u16, op_id: OpId) -> bool {
        match self.slots.get_mut(usize::from(tag)) {
            Some(slot @ None) => {
                *slot = Some(op_id.low());
                true
            }
            _ => false,
        }
    }

    /// Checks a completion against the table and frees its tag.
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
            reserved: [0; 12],
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
        e.reserved[11] = 1;
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
