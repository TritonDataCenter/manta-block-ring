//! Submission and completion entries and the request id, as plain values.

use crate::error::EntryError;
use crate::layout::{CQE_BYTES, SQE_BYTES};

/// A guest request id. The log stores it as a `u128`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OpId {
    /// New for each rust-bhyve process that attaches the volume.
    pub generation: u64,
    /// Guest queue.
    pub queue: u16,
    /// Per-queue sequence, 48 bits.
    pub seq: u64,
}

impl OpId {
    /// Largest sequence (48 bits).
    pub const MAX_SEQ: u64 = (1 << 48) - 1;

    /// The id as stored in an entry and in the log. A sequence above
    /// [`OpId::MAX_SEQ`] is cut to 48 bits; rust-bhyve never makes one.
    pub fn to_u128(self) -> u128 {
        (u128::from(self.generation) << 64) | u128::from(self.low())
    }

    /// The low 64 bits: queue and sequence.
    pub fn low(self) -> u64 {
        (u64::from(self.queue) << 48) | (self.seq & Self::MAX_SEQ)
    }

    /// Splits a stored id.
    pub fn from_u128(v: u128) -> Self {
        let low = v as u64;
        Self {
            generation: (v >> 64) as u64,
            queue: (low >> 48) as u16,
            seq: low & Self::MAX_SEQ,
        }
    }
}

/// Request ops. 4 and 5 need the `deallocate` feature.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    /// Read blocks into the buffer.
    Read = 1,
    /// Write blocks from the buffer.
    Write = 2,
    /// Flush. rust-bhyve completes it itself when writes are durable.
    Flush = 3,
    /// The range may become a hole; it reads as zeros after.
    Deallocate = 4,
    /// The range reads as zeros after.
    WriteZeroes = 5,
}

impl Op {
    /// The op for a wire value, if this build has it.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Read),
            2 => Some(Self::Write),
            3 => Some(Self::Flush),
            4 => Some(Self::Deallocate),
            5 => Some(Self::WriteZeroes),
            _ => None,
        }
    }

    /// True for an op that carries no buffer.
    pub fn is_zeroing(self) -> bool {
        matches!(self, Self::Deallocate | Self::WriteZeroes)
    }
}

/// A submission entry, unchecked; see [`crate::check::QueueState::check`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sqe {
    /// See [`OpId`].
    pub op_id: u128,
    /// First 4 KiB block.
    pub lba: u64,
    /// 4 KiB blocks.
    pub blocks: u32,
    /// First page in the queue's buffer area.
    pub buf_page: u32,
    /// Lowest sequence this queue still holds unacked.
    pub watermark: u64,
    /// rust-bhyve monotonic time in ns, for probes only.
    pub queued_at: u64,
    /// rust-bhyve's slot for the request.
    pub tag: u16,
    /// Wire value of the op.
    pub op: u8,
    /// Zero.
    pub flags: u8,
    /// Byte in the first block where the guest range starts. Zero in
    /// protocol version 1.
    pub byte_off: u16,
    /// Bytes of the guest range; 0 means every byte of `blocks`. Zero in
    /// protocol version 1.
    pub byte_len: u32,
    /// Zero.
    pub reserved: [u8; 6],
}

impl Sqe {
    /// The entry's bytes.
    pub fn encode(&self) -> [u8; SQE_BYTES] {
        let mut b = [0u8; SQE_BYTES];
        b[0..16].copy_from_slice(&self.op_id.to_le_bytes());
        b[16..24].copy_from_slice(&self.lba.to_le_bytes());
        b[24..28].copy_from_slice(&self.blocks.to_le_bytes());
        b[28..32].copy_from_slice(&self.buf_page.to_le_bytes());
        b[32..40].copy_from_slice(&self.watermark.to_le_bytes());
        b[40..48].copy_from_slice(&self.queued_at.to_le_bytes());
        b[48..50].copy_from_slice(&self.tag.to_le_bytes());
        b[50] = self.op;
        b[51] = self.flags;
        b[52..54].copy_from_slice(&self.byte_off.to_le_bytes());
        b[54..58].copy_from_slice(&self.byte_len.to_le_bytes());
        b[58..64].copy_from_slice(&self.reserved);
        b
    }

    /// The fields of an entry's bytes, unchecked.
    pub fn decode(b: &[u8; SQE_BYTES]) -> Self {
        let mut op_id = [0u8; 16];
        op_id.copy_from_slice(&b[0..16]);
        let mut reserved = [0u8; 6];
        reserved.copy_from_slice(&b[58..64]);
        Self {
            op_id: u128::from_le_bytes(op_id),
            lba: u64_at(b, 16),
            blocks: u32_at(b, 24),
            buf_page: u32_at(b, 28),
            watermark: u64_at(b, 32),
            queued_at: u64_at(b, 40),
            tag: u16::from_le_bytes([b[48], b[49]]),
            op: b[50],
            flags: b[51],
            byte_off: u16::from_le_bytes([b[52], b[53]]),
            byte_len: u32_at(b, 54),
            reserved,
        }
    }
}

/// Completion status. 4 is reserved: a request that breaks the protocol
/// gets no completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// Done; a write is durable on the ack quorum.
    Success = 0,
    /// The data could not be read or written.
    MediaError = 1,
    /// The volume is read-only.
    ReadOnly = 2,
    /// The engine does not do this op.
    Unsupported = 3,
    /// The blocks are past the end of the volume.
    OutOfRange = 5,
    /// No space to write.
    NoSpace = 6,
    /// An engine fault.
    Internal = 7,
}

impl Status {
    /// The status for a wire value, if known.
    pub fn from_u16(v: u16) -> Option<Self> {
        Some(match v {
            0 => Self::Success,
            1 => Self::MediaError,
            2 => Self::ReadOnly,
            3 => Self::Unsupported,
            5 => Self::OutOfRange,
            6 => Self::NoSpace,
            7 => Self::Internal,
            _ => return None,
        })
    }
}

/// A completion entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cqe {
    /// From the request.
    pub tag: u16,
    /// Outcome.
    pub status: Status,
    /// Low 64 bits of the request's op id.
    pub op_seq: u64,
    /// Engine time from take to completion, for probes.
    pub engine_ns: u32,
    /// Engine time spent making the write durable, for probes.
    pub durable_ns: u32,
}

impl Cqe {
    /// The entry's bytes.
    pub fn encode(&self) -> [u8; CQE_BYTES] {
        let mut b = [0u8; CQE_BYTES];
        b[0..2].copy_from_slice(&self.tag.to_le_bytes());
        b[2..4].copy_from_slice(&(self.status as u16).to_le_bytes());
        b[8..16].copy_from_slice(&self.op_seq.to_le_bytes());
        b[16..20].copy_from_slice(&self.engine_ns.to_le_bytes());
        b[20..24].copy_from_slice(&self.durable_ns.to_le_bytes());
        b
    }

    /// Reads an entry. Reserved bytes must be zero and the status known.
    pub fn decode(b: &[u8; CQE_BYTES]) -> Result<Self, EntryError> {
        if b[4..8].iter().chain(&b[24..32]).any(|x| *x != 0) {
            return Err(EntryError::Reserved);
        }
        let raw = u16::from_le_bytes([b[2], b[3]]);
        let status = Status::from_u16(raw).ok_or(EntryError::Status(raw))?;
        Ok(Self {
            tag: u16::from_le_bytes([b[0], b[1]]),
            status,
            op_seq: u64_at(b, 8),
            engine_ns: u32_at(b, 16),
            durable_ns: u32_at(b, 20),
        })
    }
}

fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]])
}

fn u64_at(b: &[u8], at: usize) -> u64 {
    let mut v = [0u8; 8];
    v.copy_from_slice(&b[at..at + 8]);
    u64::from_le_bytes(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_id_round_trip() {
        let id = OpId {
            generation: 0x0102_0304_0506_0708,
            queue: 15,
            seq: OpId::MAX_SEQ,
        };
        assert_eq!(OpId::from_u128(id.to_u128()), id);
        assert_eq!(id.low() >> 48, 15);
    }

    #[test]
    fn sqe_round_trip() {
        let e = Sqe {
            op_id: 0xdead_beef,
            lba: 9,
            blocks: 2,
            buf_page: 4,
            watermark: 1,
            queued_at: 77,
            tag: 3,
            op: 2,
            flags: 0,
            byte_off: 512,
            byte_len: 1536,
            reserved: [0; 6],
        };
        assert_eq!(Sqe::decode(&e.encode()), e);
    }

    #[test]
    fn cqe_checks_reserved_and_status() {
        let c = Cqe {
            tag: 1,
            status: Status::OutOfRange,
            op_seq: 5,
            engine_ns: 10,
            durable_ns: 4,
        };
        let b = c.encode();
        assert_eq!(Cqe::decode(&b), Ok(c));
        let mut bad = b;
        bad[30] = 1;
        assert_eq!(Cqe::decode(&bad), Err(EntryError::Reserved));
        let mut bad = b;
        bad[2] = 4;
        assert_eq!(Cqe::decode(&bad), Err(EntryError::Status(4)));
    }
}
