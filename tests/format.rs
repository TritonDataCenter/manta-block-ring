//! Frozen bytes of layout version 1 and protocol version 1. If one of
//! these fails, add a version or a feature bit; do not change the bytes.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use manta_block_ring::control::{ATTACH_DEALLOCATE, ATTACH_DURABLE, Attach, AttachOk, Message};
use manta_block_ring::{Cqe, Geometry, Header, OpId, Sqe, Status};

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[test]
fn header_bytes() {
    let h = Header {
        geometry: Geometry::new(2, 256, 64).unwrap(),
        volume_blocks: 0x0102_0304,
        attach_generation: 0x0a0b,
    };
    assert_eq!(hex(&h.encode()), HEADER);
}

/// A ring smaller than a page still takes a whole page.
#[test]
fn small_ring_header_bytes() {
    let h = Header {
        geometry: Geometry::new(1, 16, 2).unwrap(),
        volume_blocks: 0x0102_0304,
        attach_generation: 0x0a0b,
    };
    assert_eq!(hex(&h.encode()), SMALL_HEADER);
}

#[test]
fn entry_bytes() {
    let e = Sqe {
        op_id: OpId {
            generation: 0x1122_3344_5566_7788,
            queue: 3,
            seq: 0x0000_aabb_ccdd,
        }
        .to_u128(),
        lba: 0x10,
        blocks: 2,
        buf_page: 5,
        watermark: 0x0000_aabb_cc00,
        queued_at: 0x99,
        tag: 7,
        op: 2,
        flags: 0,
        byte_off: 0,
        byte_len: 0,
        reserved: [0; 6],
    };
    assert_eq!(hex(&e.encode()), SQE);
    let c = Cqe {
        tag: 7,
        status: Status::OutOfRange,
        op_seq: 0x0003_0000_aabb_ccdd,
        engine_ns: 0x1234,
        durable_ns: 0x56,
    };
    assert_eq!(hex(&c.encode()), CQE);
}

#[test]
fn control_bytes() {
    let attach = Message::Attach(Attach {
        volume: *b"0123456789abcdef",
        generation: 0,
        queues: 4,
        depth: 256,
        buf_pages: 512,
        max_blocks: 512,
    })
    .encode(1);
    assert_eq!(hex(&attach), ATTACH);
    let ok = Message::AttachOk(AttachOk {
        generation: 9,
        queues: 4,
        depth: 256,
        buf_pages: 512,
        max_blocks: 512,
        volume_blocks: 1 << 20,
        region_len: 0x0218_1000,
        layout_version: 1,
        flags: ATTACH_DURABLE,
        sector_bytes: 4096,
    })
    .encode(1);
    assert_eq!(hex(&ok), ATTACH_OK);
    assert_eq!(hex(&Message::Ping.encode(5)), PING);
}

/// Protocol version 2: the byte range of a sub-block write, and the
/// sector size in AttachOk.
#[test]
fn version_2_bytes() {
    let e = Sqe {
        op_id: OpId {
            generation: 0x1122_3344_5566_7788,
            queue: 3,
            seq: 0x0000_aabb_ccdd,
        }
        .to_u128(),
        lba: 0x10,
        blocks: 2,
        buf_page: 5,
        watermark: 0x0000_aabb_cc00,
        queued_at: 0x99,
        tag: 7,
        op: 2,
        flags: 0,
        byte_off: 0x0600,
        byte_len: 0x1200,
        reserved: [0; 6],
    };
    assert_eq!(hex(&e.encode()), SQE_V2);
    let ok = Message::AttachOk(AttachOk {
        generation: 9,
        queues: 4,
        depth: 256,
        buf_pages: 512,
        max_blocks: 512,
        volume_blocks: 1 << 20,
        region_len: 0x0218_1000,
        layout_version: 1,
        flags: ATTACH_DURABLE | ATTACH_DEALLOCATE,
        sector_bytes: 512,
    })
    .encode_as(1, 2);
    assert_eq!(hex(&ok), ATTACH_OK_V2);
}

const HEADER: &str = "4d42524701000200000100004000000000f0080000000000007004000000000004030201000000000b0a000000000000";
const SMALL_HEADER: &str = "4d4252470100010010000000020000000060000000000000005000000000000004030201000000000b0a000000000000";
const SQE: &str = "ddccbbaa0000030088776655443322111000000000000000020000000500000000ccbbaa00000000990000000000000007000200000000000000000000000000";
const CQE: &str = "0700050000000000ddccbbaa0000030034120000560000000000000000000000";
const ATTACH: &str = "3200000003000100010000003031323334353637383961626364656600000000000000000400000100000002000000020000";
const ATTACH_OK: &str = "360000000400010001000000090000000000000004000001000000020000000200000000100000000000001018020000000001000100";
const SQE_V2: &str = "ddccbbaa0000030088776655443322111000000000000000020000000500000000ccbbaa00000000990000000000000007000200000600120000000000000000";
const ATTACH_OK_V2: &str = "3a000000040002000100000009000000000000000400000100000002000000020000000010000000000000101802000000000100030000020000";
const PING: &str = "0c0000000b00010005000000";
