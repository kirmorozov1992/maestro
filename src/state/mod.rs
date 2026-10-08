//! In-memory control-plane state.
//!
//! A single lock protects every collection so store operations can update jobs,
//! allocations, reservations, and queue membership atomically. Lock sections
//! must stay short and must not include async or external work.

pub(crate) mod error;
pub(crate) mod store;
