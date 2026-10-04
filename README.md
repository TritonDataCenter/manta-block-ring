# manta-block-ring

The shared-memory ring between [rust-bhyve](https://github.com/TritonDataCenter/rust-bhyve) and the MantaBlock storage engine. Both sides use this crate, so the layout, the entry formats, the ring operations and the checks exist once.

The crate has no dependencies and does no I/O. Each side maps the region, passes descriptors, and runs its own sockets and pipes.

## Model

- The engine and the VMM share one region: a tmpfs file that the engine creates and sends over a Unix socket with `SCM_RIGHTS`.
- Each guest queue has one submission ring (SQ, 64-byte entries) and one completion ring (CQ, 32-byte entries), plus a buffer area. The engine never maps guest memory. The VMM copies data into and out of the buffer area.
- Each side sleeps on its own pipe. A side sends a wake-up only when the other side says it is idle.
- The engine treats everything the VMM writes as hostile. Only `ring.rs` touches shared memory: atomics for the rings, and an inline-asm copy for the buffer area. Every request goes through `QueueState::check` before the engine uses it.

## Region layout, version 1

| Part | Size |
|---|---|
| Header | 4 KiB |
| For each queue: control page | 4 KiB |
| For each queue: SQ | `depth` × 64 bytes, padded to 4 KiB |
| For each queue: CQ | `depth` × 32 bytes, padded to 4 KiB |
| For each queue: buffer area | `buf_pages` × 4 KiB |

`Geometry` computes every offset with checked arithmetic. `Header::encode` and `Header::decode` define the header bytes. Tests in `tests/format.rs` freeze the bytes of the header and the entries, so a change to the format fails a test.

## Request ids

`op_id = generation << 64 | queue << 48 | sequence`. The engine issues the generation when the VMM attaches. A sequence is never used twice in one generation.

## Control protocol

`control.rs` defines the framed messages on the Unix socket: Hello, Attach, AttachOk (with descriptors), Pause, Resume, Detach, Ping and Error.

## Versions

The region header and the control frames carry versions. A side that does not know a version refuses it, and never guesses a layout. Reserved fields are 0 in version 1, and a side that sees a non-zero reserved field refuses the message.

## License

MPL-2.0. See `LICENSE`.
