//! Shared, non-module tenancy for SupportGenius ventures: signed tenant
//! API keys, issued here rather than inside any one module so every
//! venture and sidecar verifies the same credential format.
//!
//! The keys will be built on `cratefield_core`'s `Signer` port (the same
//! HMAC-backed port the runtime assembles from the `HARNESS_SECRET`
//! secret), kept out of the module boundary so modules consume tenancy
//! through ports and never issue keys themselves.
//!
//! **Nothing is implemented yet.** A later issue fills this crate in.
