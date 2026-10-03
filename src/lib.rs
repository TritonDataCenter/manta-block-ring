//! The shared-memory ring between rust-bhyve and the MantaBlock engine
//! (`docs/plan/37-mantablock-ring-spec.md`).
//!
//! Both sides use this crate, so the layout, the entry formats, the ring
//! operations and the checks exist once. It has no dependencies and does no
//! I/O: each side maps the region, passes descriptors and runs its own
//! sockets and pipes.
//!
//! The engine treats everything rust-bhyve writes as hostile. Only `ring`
//! touches shared memory, and only with atomics; every request goes through
//! [`check::QueueState::check`] before the engine uses it.

pub mod check;
pub mod control;
pub mod entry;
pub mod error;
pub mod layout;
pub mod ring;

pub use check::{Limits, Outstanding, QueueState, Request};
pub use entry::{Cqe, Op, OpId, Sqe, Status};
pub use error::{BadCompletion, Broken, ControlError, EntryError, LayoutError, Reject};
pub use layout::{Geometry, Header};
pub use ring::{Consumer, Producer, Region};
