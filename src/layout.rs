//! Region geometry and the region header.
//!
//! ```text
//! page 0                    region header
//! for each queue q:
//!   1 page                  queue control (indices and flags)
//!   depth × 64 B            submission ring, padded to a page
//!   depth × 32 B            completion ring, padded to a page
//!   buf_pages × 4 KiB       buffer area
//! ```
//!
//! Both sides compute every offset from the agreed geometry and never trust
//! a size read from the region.

use crate::error::LayoutError;

/// Page size of the region, and the size of one block of volume data.
pub const PAGE: usize = 4096;

/// "MBRG" read as a little-endian `u32`.
pub const MAGIC: u32 = u32::from_le_bytes(*b"MBRG");

/// Region layout version this crate reads and writes.
pub const LAYOUT_VERSION: u16 = 1;

/// Most queues per attachment.
pub const MAX_QUEUES: u16 = 16;

/// Fewest and most entries per ring.
pub const MIN_DEPTH: u32 = 2;
/// See [`MIN_DEPTH`].
pub const MAX_DEPTH: u32 = 4096;

/// 4 GiB per queue. The engine grants far fewer; this only bounds the
/// arithmetic.
pub const MAX_BUF_PAGES: u32 = 1 << 20;

/// Bytes of one submission entry.
pub const SQE_BYTES: usize = 64;
/// Bytes of one completion entry.
pub const CQE_BYTES: usize = 32;

/// Offsets of the fields in a queue control page. Each is a 32-bit atomic
/// on its own 64-byte cache line.
pub mod control {
    /// Written by rust-bhyve.
    pub const SQ_TAIL: usize = 0;
    /// Written by the engine.
    pub const SQ_HEAD: usize = 64;
    /// Written by the engine.
    pub const CQ_TAIL: usize = 128;
    /// Written by rust-bhyve.
    pub const CQ_HEAD: usize = 192;
    /// Written by the engine: 1 while it waits for a wake-up.
    pub const ENGINE_IDLE: usize = 256;
    /// Written by rust-bhyve: 1 while it waits for a wake-up.
    pub const VMM_IDLE: usize = 320;
}

/// The shape of one region: what both sides agree on at attach.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Geometry {
    queues: u16,
    depth: u32,
    buf_pages: u32,
    sq_bytes: usize,
    queue_bytes: usize,
    region_len: usize,
}

impl Geometry {
    /// Checks the attach parameters and computes the layout.
    pub fn new(queues: u16, depth: u32, buf_pages: u32) -> Result<Self, LayoutError> {
        if queues == 0 || queues > MAX_QUEUES {
            return Err(LayoutError::Queues(queues));
        }
        if !(MIN_DEPTH..=MAX_DEPTH).contains(&depth) || !depth.is_power_of_two() {
            return Err(LayoutError::Depth(depth));
        }
        if buf_pages == 0 || buf_pages > MAX_BUF_PAGES {
            return Err(LayoutError::BufPages(buf_pages));
        }
        let sq_bytes = ring_bytes(depth, SQE_BYTES).ok_or(LayoutError::TooLarge)?;
        let cq_bytes = ring_bytes(depth, CQE_BYTES).ok_or(LayoutError::TooLarge)?;
        let queue_bytes = (buf_pages as usize)
            .checked_mul(PAGE)
            .and_then(|b| b.checked_add(PAGE + sq_bytes + cq_bytes))
            .ok_or(LayoutError::TooLarge)?;
        let region_len = queue_bytes
            .checked_mul(usize::from(queues))
            .and_then(|b| b.checked_add(PAGE))
            .ok_or(LayoutError::TooLarge)?;
        Ok(Self {
            queues,
            depth,
            buf_pages,
            sq_bytes,
            queue_bytes,
            region_len,
        })
    }

    /// Number of queues.
    pub fn queues(&self) -> u16 {
        self.queues
    }

    /// Entries per ring.
    pub fn depth(&self) -> u32 {
        self.depth
    }

    /// Buffer pages per queue.
    pub fn buf_pages(&self) -> u32 {
        self.buf_pages
    }

    /// Bytes per queue block.
    pub fn queue_bytes(&self) -> usize {
        self.queue_bytes
    }

    /// Bytes of the whole region.
    pub fn region_len(&self) -> usize {
        self.region_len
    }

    /// Offset of queue `q`'s control page. `q` must be below `queues`.
    pub(crate) fn queue_offset(&self, q: u16) -> usize {
        PAGE + usize::from(q) * self.queue_bytes
    }

    /// Offset of queue `q`'s submission ring.
    pub(crate) fn sq_offset(&self, q: u16) -> usize {
        self.queue_offset(q) + PAGE
    }

    /// Offset of queue `q`'s completion ring.
    pub(crate) fn cq_offset(&self, q: u16) -> usize {
        self.sq_offset(q) + self.sq_bytes
    }

    /// Offset of queue `q`'s buffer area.
    pub(crate) fn buf_offset(&self, q: u16) -> usize {
        self.queue_offset(q) + self.queue_bytes - self.buf_pages as usize * PAGE
    }
}

/// Bytes of one ring, padded to a page so the next block is page-aligned.
fn ring_bytes(depth: u32, entry: usize) -> Option<usize> {
    (depth as usize)
        .checked_mul(entry)?
        .checked_add(PAGE - 1)
        .map(|v| v & !(PAGE - 1))
}

/// The header in page 0. Informative only: each side keeps the agreed
/// geometry in private memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    /// The agreed geometry.
    pub geometry: Geometry,
    /// Volume size in 4 KiB blocks.
    pub volume_blocks: u64,
    /// High 64 bits of every op id in this attachment.
    pub attach_generation: u64,
}

/// Bytes of the header that carry fields; the rest of page 0 is zero.
pub const HEADER_BYTES: usize = 48;

impl Header {
    /// The header bytes.
    pub fn encode(&self) -> [u8; HEADER_BYTES] {
        let g = &self.geometry;
        let mut b = [0u8; HEADER_BYTES];
        b[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        b[4..6].copy_from_slice(&LAYOUT_VERSION.to_le_bytes());
        b[6..8].copy_from_slice(&g.queues.to_le_bytes());
        b[8..12].copy_from_slice(&g.depth.to_le_bytes());
        b[12..16].copy_from_slice(&g.buf_pages.to_le_bytes());
        b[16..24].copy_from_slice(&(g.region_len as u64).to_le_bytes());
        b[24..32].copy_from_slice(&(g.queue_bytes as u64).to_le_bytes());
        b[32..40].copy_from_slice(&self.volume_blocks.to_le_bytes());
        b[40..48].copy_from_slice(&self.attach_generation.to_le_bytes());
        b
    }

    /// The stated sizes must match the ones the geometry gives.
    pub fn decode(b: &[u8; HEADER_BYTES]) -> Result<Self, LayoutError> {
        let u16_at = |at: usize| u16::from_le_bytes([b[at], b[at + 1]]);
        let u32_at = |at: usize| u32::from_le_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]]);
        let u64_at = |at: usize| {
            let mut v = [0u8; 8];
            v.copy_from_slice(&b[at..at + 8]);
            u64::from_le_bytes(v)
        };
        let magic = u32_at(0);
        if magic != MAGIC {
            return Err(LayoutError::Magic(magic));
        }
        let version = u16_at(4);
        if version != LAYOUT_VERSION {
            return Err(LayoutError::Version(version));
        }
        let geometry = Geometry::new(u16_at(6), u32_at(8), u32_at(12))?;
        if u64_at(16) != geometry.region_len as u64 || u64_at(24) != geometry.queue_bytes as u64 {
            return Err(LayoutError::SizeMismatch);
        }
        Ok(Self {
            geometry,
            volume_blocks: u64_at(32),
            attach_generation: u64_at(40),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offsets_are_page_aligned_and_inside_the_region() {
        let g = Geometry::new(3, 1024, 256).unwrap();
        for q in 0..3 {
            for off in [
                g.queue_offset(q),
                g.sq_offset(q),
                g.cq_offset(q),
                g.buf_offset(q),
            ] {
                assert_eq!(off % PAGE, 0);
            }
            assert!(g.cq_offset(q) + 1024 * CQE_BYTES <= g.buf_offset(q));
            assert_eq!(
                g.buf_offset(q) + 256 * PAGE,
                g.queue_offset(q) + g.queue_bytes()
            );
        }
        assert_eq!(g.region_len(), PAGE + 3 * g.queue_bytes());
        // 1024 x 96 B = 24 pages of rings.
        assert_eq!(g.queue_bytes(), (1 + 24 + 256) * PAGE);
    }

    #[test]
    fn every_block_is_page_aligned_at_every_depth() {
        let mut depth = MIN_DEPTH;
        while depth <= MAX_DEPTH {
            let g = Geometry::new(2, depth, 1).unwrap();
            for q in 0..2 {
                for off in [
                    g.queue_offset(q),
                    g.sq_offset(q),
                    g.cq_offset(q),
                    g.buf_offset(q),
                ] {
                    assert_eq!(off % PAGE, 0, "depth {depth}");
                }
                assert!(g.sq_offset(q) + depth as usize * SQE_BYTES <= g.cq_offset(q));
                assert!(g.cq_offset(q) + depth as usize * CQE_BYTES <= g.buf_offset(q));
            }
            depth *= 2;
        }
        // Small rings take a page each.
        assert_eq!(Geometry::new(1, 16, 1).unwrap().queue_bytes(), 4 * PAGE);
    }

    #[test]
    fn rejects_bad_geometry() {
        assert_eq!(Geometry::new(0, 8, 1), Err(LayoutError::Queues(0)));
        assert_eq!(Geometry::new(17, 8, 1), Err(LayoutError::Queues(17)));
        assert_eq!(Geometry::new(1, 6, 1), Err(LayoutError::Depth(6)));
        assert_eq!(Geometry::new(1, 8192, 1), Err(LayoutError::Depth(8192)));
        assert_eq!(Geometry::new(1, 8, 0), Err(LayoutError::BufPages(0)));
        assert_eq!(
            Geometry::new(1, 8, MAX_BUF_PAGES + 1),
            Err(LayoutError::BufPages(MAX_BUF_PAGES + 1))
        );
    }

    #[test]
    fn header_round_trip_and_checks() {
        let h = Header {
            geometry: Geometry::new(2, 64, 32).unwrap(),
            volume_blocks: 1 << 20,
            attach_generation: 7,
        };
        let b = h.encode();
        assert_eq!(Header::decode(&b), Ok(h));
        let mut bad = b;
        bad[16] ^= 1;
        assert_eq!(Header::decode(&bad), Err(LayoutError::SizeMismatch));
        let mut bad = b;
        bad[4] = 2;
        assert_eq!(Header::decode(&bad), Err(LayoutError::Version(2)));
        let mut bad = b;
        bad[0] = 0;
        assert!(matches!(Header::decode(&bad), Err(LayoutError::Magic(_))));
    }
}
