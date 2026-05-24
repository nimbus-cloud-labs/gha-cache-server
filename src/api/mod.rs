//! Aggregates the HTTP and Twirp API surfaces for cache interactions.
//!
//! This module coordinates request handling, type conversions, and compatibility
//! shims exposed to API consumers. Submodules provide endpoint-specific
//! handlers, protobuf bindings, and helpers for translating Twirp payloads into
//! the internal domain types used by the server.
pub mod artifact;
pub mod download;
pub mod path;
pub mod proto;
pub mod proxy;
pub mod twirp;
pub mod types;
pub mod upload;
pub mod upload_compat;
