//! Random input from a hostile peer: nothing panics, and whatever the
//! checks accept stays inside the agreed limits.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use manta_block_ring::control::Message;
use manta_block_ring::layout::{HEADER_BYTES, SQE_BYTES};
use manta_block_ring::{Cqe, Header, Limits, Op, QueueState, Sqe};
use proptest::prelude::*;

fn limits() -> Limits {
    Limits {
        generation: 3,
        depth: 64,
        buf_pages: 128,
        volume_blocks: 1 << 30,
        max_blocks: 512,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(5000))]

    #[test]
    fn control_decode_never_panics(b in prop::collection::vec(any::<u8>(), 0..300)) {
        let _ = Message::decode(&b);
    }

    #[test]
    fn decoded_frames_encode_back_to_the_same_bytes(b in prop::collection::vec(any::<u8>(), 0..300)) {
        if let Ok(Some((m, id, used))) = Message::decode(&b) {
            prop_assert_eq!(m.encode(id), b[..used].to_vec());
        }
    }

    #[test]
    fn header_decode_never_panics(b in prop::array::uniform32(any::<u8>()), c in prop::array::uniform16(any::<u8>())) {
        let mut h = [0u8; HEADER_BYTES];
        h[..32].copy_from_slice(&b);
        h[32..].copy_from_slice(&c);
        let _ = Header::decode(&h);
    }

    #[test]
    fn cqe_decode_never_panics(b in prop::array::uniform32(any::<u8>())) {
        let _ = Cqe::decode(&b);
    }

    #[test]
    fn accepted_requests_stay_inside_the_limits(
        raw in prop::collection::vec(prop::collection::vec(any::<u8>(), SQE_BYTES), 1..40),
        steer in any::<bool>(),
    ) {
        let l = limits();
        let mut q = QueueState::new(0, l, 0);
        for bytes in raw {
            let mut b = [0u8; SQE_BYTES];
            b.copy_from_slice(&bytes);
            if steer {
                // Pass the cheap checks so the range and buffer checks run.
                b[8..16].copy_from_slice(&3u64.to_le_bytes());
                b[6..8].copy_from_slice(&0u16.to_le_bytes());
                b[50] = 1 + b[50] % 3;
                b[51] = 0;
                b[52..64].fill(0);
            }
            if !q.can_take() {
                break;
            }
            let Ok(r) = q.check(&Sqe::decode(&b)) else { continue };
            prop_assert!(u32::from(r.tag) < l.depth);
            prop_assert_eq!(r.op_id.generation, l.generation);
            prop_assert_eq!(r.op_id.queue, 0);
            if r.op != Op::Flush {
                prop_assert!(r.blocks >= 1 && r.blocks <= l.max_blocks);
                prop_assert!(r.lba + u64::from(r.blocks) <= l.volume_blocks);
                prop_assert!(u64::from(r.buf_page) + u64::from(r.blocks) <= u64::from(l.buf_pages));
            }
        }
    }
}
