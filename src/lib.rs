//! Declared roles: facade

pub mod account;
pub mod discovery;
pub mod dispatch;
pub mod encoding;
pub mod envelope;
pub mod schema;

pub use dispatch::handle_invocation;
