//! Errors. Each says what the other side did wrong.

use std::fmt;

/// The geometry or the region header is not valid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayoutError {
    /// Queues outside 1 to 16.
    Queues(u16),
    /// Depth not a power of 2 in 2 to 4096.
    Depth(u32),
    /// Buffer pages outside the layout's range.
    BufPages(u32),
    /// The region would not fit in memory.
    TooLarge,
    /// The header does not start with the magic.
    Magic(u32),
    /// A layout version this build does not read.
    Version(u16),
    /// The header's sizes are not the ones its geometry gives.
    SizeMismatch,
    /// The mapping is shorter than the geometry needs, or not page-aligned.
    Mapping,
}

/// An entry breaks the format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryError {
    /// A reserved byte or the flags field is not zero.
    Reserved,
    /// A status this build does not know.
    Status(u16),
}

/// Why the engine refuses a request. A correct rust-bhyve never causes one;
/// the engine sends Error and detaches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reject {
    /// The op is not read, write or flush.
    Op(u8),
    /// Flags or reserved bytes are not zero.
    Reserved,
    /// The tag is not below the depth.
    Tag(u16),
    /// A request with this tag is already in flight.
    TagInFlight(u16),
    /// The op id names another attachment generation.
    Generation(u64),
    /// The op id names another queue.
    Queue(u16),
    /// The sequence does not go up.
    SeqNotIncreasing {
        /// Highest sequence taken before.
        last: u64,
        /// Sequence in the request.
        found: u64,
    },
    /// The sequence is below the queue's floor.
    BelowFloor {
        /// The floor.
        floor: u64,
        /// Sequence in the request.
        found: u64,
    },
    /// The watermark is above the request's own sequence.
    Watermark,
    /// Zero blocks, or more than one request may carry.
    Blocks(u32),
    /// The blocks run past the end of the volume.
    Range,
    /// The data runs past the queue's buffer area.
    Buffer,
    /// A flush with a block range or a buffer.
    Flush,
}

/// An index moved in a way no correct peer moves it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Broken;

/// A completion that does not match an outstanding request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BadCompletion {
    /// The tag is not below the depth, or nothing is in flight with it.
    Tag(u16),
    /// The op id does not match the request with that tag.
    OpSeq,
    /// The status is not known.
    Entry(EntryError),
}

/// A control message breaks the protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlError {
    /// The length is below the frame header or above 4 KiB.
    Length(u32),
    /// A message kind this build does not know.
    Kind(u16),
    /// A protocol version this build does not speak.
    Version(u16),
    /// The body is not the size its kind needs, or its text is not UTF-8.
    Body,
}

macro_rules! display_debug {
    ($($t:ty),*) => {$(
        impl fmt::Display for $t {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Debug::fmt(self, f)
            }
        }
        impl std::error::Error for $t {}
    )*};
}

display_debug!(
    LayoutError,
    EntryError,
    Reject,
    Broken,
    BadCompletion,
    ControlError
);
