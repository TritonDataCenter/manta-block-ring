//! The shared-memory ring between rust-bhyve and the MantaBlock engine.
//!
//! No dependencies and no I/O: each side maps the region and runs its own
//! sockets and pipes. The engine treats everything rust-bhyve writes as
//! hostile, so every request goes through [`check::QueueState::check`].

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
