//! The ring between two threads that play rust-bhyve and the engine, and
//! against a peer that writes garbage.

#![allow(unsafe_code, clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::alloc::{Layout, alloc_zeroed};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::thread;
use std::time::Duration;

use manta_block_ring::layout::{PAGE, control};
use manta_block_ring::{
    Broken, Cqe, Geometry, Header, Limits, OpId, Outstanding, QueueState, Region, Sqe, Status,
};

const GEN: u64 = 0x5eed;

/// A zeroed, page-aligned region that is never freed, so it outlives every
/// handle. Returns the region and the raw base for tests that play a
/// hostile peer.
fn region(g: Geometry) -> (Arc<Region>, usize) {
    let layout = Layout::from_size_align(g.region_len(), PAGE).unwrap();
    // SAFETY: the layout has a non-zero size.
    let base = unsafe { alloc_zeroed(layout) };
    assert!(!base.is_null());
    // SAFETY: `base` is a fresh allocation of `region_len` bytes that is
    // never freed; tests touch it only through the Region or with atomics.
    let r = unsafe { Region::from_raw_parts(base, g.region_len(), g) }.unwrap();
    (Arc::new(r), base as usize)
}

/// Writes a u32 into the region as the other process would.
fn poke(base: usize, off: usize, v: u32) {
    // SAFETY: `off` is inside the test region and 4-byte aligned, and the
    // region is only accessed with atomics.
    unsafe { std::sync::atomic::AtomicU32::from_ptr((base + off) as *mut u32) }
        .store(v, Ordering::SeqCst);
}

fn sqe(seq: u64, tag: u16, buf_page: u32, watermark: u64) -> Sqe {
    Sqe {
        op_id: OpId {
            generation: GEN,
            queue: 0,
            seq,
        }
        .to_u128(),
        lba: seq % 1000,
        blocks: 1,
        buf_page,
        watermark,
        queued_at: 0,
        tag,
        op: 2,
        flags: 0,
        reserved: [0; 12],
    }
}

#[test]
fn entries_wrap_around_the_ring_in_order() {
    let g = Geometry::new(1, 4, 4).unwrap();
    let (r, _) = region(g);
    let mut sq = r.sq_producer(0).unwrap();
    let mut eng = r.sq_consumer(0).unwrap();
    for round in 0..5u64 {
        for i in 0..4 {
            assert!(
                sq.try_push(&sqe(round * 4 + i, i as u16, 0, 0).encode())
                    .unwrap()
            );
        }
        assert!(!sq.try_push(&sqe(99, 0, 0, 0).encode()).unwrap(), "full");
        sq.publish();
        for i in 0..4 {
            let e = Sqe::decode(&eng.pop().unwrap().unwrap());
            assert_eq!(OpId::from_u128(e.op_id).seq, round * 4 + i);
        }
        assert_eq!(eng.pop().unwrap(), None);
        eng.release();
    }
}

#[test]
fn a_tail_past_the_depth_or_backwards_is_broken() {
    let g = Geometry::new(2, 8, 1).unwrap();
    let (r, base) = region(g);
    let ctl = PAGE + g.queue_bytes();
    let mut eng = r.sq_consumer(1).unwrap();
    poke(base, ctl + control::SQ_TAIL, 9);
    assert_eq!(eng.pop(), Err(Broken));
    poke(base, ctl + control::SQ_TAIL, 3);
    for _ in 0..3 {
        assert!(eng.pop().unwrap().is_some());
    }
    poke(base, ctl + control::SQ_TAIL, 2);
    assert_eq!(eng.pop(), Err(Broken));
}

#[test]
fn a_head_that_jumps_is_broken() {
    let g = Geometry::new(1, 4, 1).unwrap();
    let (r, base) = region(g);
    let mut cq = r.cq_producer(0).unwrap();
    let c = Cqe {
        tag: 0,
        status: Status::Success,
        op_seq: 0,
        engine_ns: 0,
        durable_ns: 0,
    }
    .encode();
    for _ in 0..4 {
        assert!(cq.try_push(&c).unwrap());
    }
    // The head may not pass the tail.
    poke(base, PAGE + control::CQ_HEAD, 5);
    assert_eq!(cq.try_push(&c), Err(Broken));
}

#[test]
fn buffers_stay_inside_their_queue() {
    let g = Geometry::new(2, 2, 3).unwrap();
    let (r, _) = region(g);
    let data = vec![0xabu8; 2 * PAGE];
    assert!(r.write_buffer(1, 1, &data));
    let mut out = vec![0u8; 2 * PAGE];
    assert!(r.read_buffer(1, 1, &mut out));
    assert_eq!(out, data);
    assert!(!r.write_buffer(1, 2, &data), "runs past the area");
    assert!(!r.write_buffer(2, 0, &data[..8]), "no such queue");
    assert!(!r.read_buffer(0, u32::MAX, &mut out[..8]));
    assert!(!r.write_buffer(0, 0, &data[..7]), "not a multiple of 8");
}

#[test]
fn header_written_by_the_engine_reads_back() {
    let g = Geometry::new(3, 16, 2).unwrap();
    let (r, _) = region(g);
    let h = Header {
        geometry: g,
        volume_blocks: 123,
        attach_generation: GEN,
    };
    r.write_header(&h);
    assert_eq!(r.read_header(), Ok(h));
}

/// A wake-up channel with pipe semantics: a full channel means a wake-up
/// is already waiting.
fn pipe() -> (SyncSender<()>, Receiver<()>) {
    sync_channel(1)
}

fn wake(tx: &SyncSender<()>) {
    match tx.try_send(()) {
        Ok(()) | Err(TrySendError::Full(())) => {}
        Err(TrySendError::Disconnected(())) => {}
    }
}

/// Blocks until woken. A lost wake-up shows up as a timeout.
fn wait(rx: &Receiver<()>) {
    rx.recv_timeout(Duration::from_secs(10))
        .expect("lost wake-up: nobody woke the waiting side");
}

#[test]
fn two_threads_move_every_request_and_completion() {
    const N: u64 = 200_000;
    let depth = 64;
    let g = Geometry::new(1, depth, depth).unwrap();
    let (r, _) = region(g);
    let (wake_engine, engine_rx) = pipe();
    let (wake_vmm, vmm_rx) = pipe();

    let er = Arc::clone(&r);
    let engine = thread::spawn(move || {
        let mut sq = er.sq_consumer(0).unwrap();
        let mut cq = er.cq_producer(0).unwrap();
        let limits = Limits {
            generation: GEN,
            depth,
            buf_pages: depth,
            volume_blocks: 1000,
            max_blocks: 1,
        };
        let mut q = QueueState::new(0, limits, 0);
        let mut done = 0u64;
        let mut page = vec![0u8; PAGE];
        while done < N {
            let mut worked = false;
            while q.can_take() {
                let Some(b) = sq.pop().unwrap() else { break };
                let req = q.check(&Sqe::decode(&b)).unwrap();
                assert!(er.read_buffer(0, req.buf_page, &mut page));
                assert!(page.iter().all(|x| *x == req.op_id.seq as u8), "data");
                let c = Cqe {
                    tag: req.tag,
                    status: Status::Success,
                    op_seq: req.op_id.low(),
                    engine_ns: 0,
                    durable_ns: 0,
                };
                while !cq.try_push(&c.encode()).unwrap() {
                    std::hint::spin_loop();
                }
                assert!(q.complete(req.tag));
                done += 1;
                worked = true;
            }
            sq.release();
            if cq.publish() {
                wake(&wake_vmm);
            }
            // A tiny spin budget, so both sides sleep often.
            if !worked && sq.prepare_wait().unwrap() {
                wait(&engine_rx);
                sq.woke();
            }
        }
    });

    let mut sq = r.sq_producer(0).unwrap();
    let mut cq = r.cq_consumer(0).unwrap();
    let mut out = Outstanding::new(depth);
    let mut next = 0u64;
    let mut completed = 0u64;
    while completed < N {
        // Submit while there is a free tag.
        let mut sent = false;
        while next < N {
            let Some(tag) = out.free_tag() else { break };
            let page = vec![next as u8; PAGE];
            assert!(r.write_buffer(0, u32::from(tag), &page));
            let id = OpId {
                generation: GEN,
                queue: 0,
                seq: next,
            };
            assert!(
                sq.try_push(&sqe(next, tag, u32::from(tag), completed).encode())
                    .unwrap()
            );
            assert!(out.insert(tag, id));
            next += 1;
            sent = true;
        }
        if sent && sq.publish() {
            wake(&wake_engine);
        }
        let mut got = false;
        while let Some(b) = cq.pop().unwrap() {
            let c = Cqe::decode(&b).unwrap();
            out.complete(&c).unwrap();
            completed += 1;
            got = true;
        }
        cq.release();
        if !got && !sent && cq.prepare_wait().unwrap() {
            wait(&vmm_rx);
            cq.woke();
        }
    }
    engine.join().unwrap();
    assert!(out.is_empty());
}

#[test]
fn a_peer_writing_garbage_never_panics_the_engine() {
    let depth = 16;
    let g = Geometry::new(1, depth, depth).unwrap();
    let (r, base) = region(g);
    let stop = Arc::new(AtomicBool::new(false));
    let s = Arc::clone(&stop);
    let len = g.region_len();
    let hostile = thread::spawn(move || {
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        while !s.load(Ordering::Relaxed) {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            // Anywhere in the control page and the rings, 4-byte aligned.
            let off = (PAGE + (x as usize) % (3 * PAGE)) & !3;
            if off + 4 <= len {
                poke(base, off, (x >> 32) as u32);
            }
        }
    });
    let limits = Limits {
        generation: GEN,
        depth,
        buf_pages: depth,
        volume_blocks: 1 << 20,
        max_blocks: 4,
    };
    let (mut ok, mut rejected, mut broken) = (0, 0, 0);
    for _ in 0..2_000 {
        let mut sq = r.sq_consumer(0).unwrap();
        let mut q = QueueState::new(0, limits, 0);
        for _ in 0..64 {
            match sq.pop() {
                Err(Broken) => {
                    broken += 1;
                    break;
                }
                Ok(None) => {}
                Ok(Some(b)) => match q.check(&Sqe::decode(&b)) {
                    Ok(req) => {
                        ok += 1;
                        assert!(req.lba + u64::from(req.blocks) <= 1 << 20);
                        q.complete(req.tag);
                    }
                    Err(_) => rejected += 1,
                },
            }
        }
    }
    stop.store(true, Ordering::Relaxed);
    hostile.join().unwrap();
    // Garbage is refused or seen as a broken peer; it never panics.
    assert!(rejected + broken > 0, "the hostile writer had no effect");
    let _ = ok;
}

/// The interleaving a lost wake-up needs, step by step: the consumer finds
/// the ring empty, the producer publishes before the consumer sets its idle
/// flag (so it sends no wake-up), then the consumer gets ready to sleep. It
/// must see the new entry instead of sleeping.
#[test]
fn the_consumer_rechecks_before_it_sleeps() {
    let g = Geometry::new(1, 4, 1).unwrap();
    let (r, _) = region(g);
    let mut sq = r.sq_producer(0).unwrap();
    let mut eng = r.sq_consumer(0).unwrap();
    assert_eq!(eng.pop().unwrap(), None);
    assert!(sq.try_push(&sqe(0, 0, 0, 0).encode()).unwrap());
    assert!(!sq.publish(), "the consumer is not idle yet");
    assert!(
        !eng.prepare_wait().unwrap(),
        "must not sleep: an entry is waiting"
    );
    assert!(eng.pop().unwrap().is_some());

    // The other order: the consumer is idle first, so the producer must
    // ask for a wake-up.
    assert_eq!(eng.pop().unwrap(), None);
    assert!(eng.prepare_wait().unwrap());
    assert!(sq.try_push(&sqe(1, 1, 0, 0).encode()).unwrap());
    assert!(sq.publish(), "the consumer sleeps and needs a wake-up");
    eng.woke();
    assert!(eng.pop().unwrap().is_some());
}
