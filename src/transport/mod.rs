//! HTTP routes and transport DTOs.
//!
//! The versioned API is implemented in M5.

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "M5.2 transport handlers will consume the versioned DTOs"
    )
)]
pub(crate) mod v1;
