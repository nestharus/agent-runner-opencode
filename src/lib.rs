//! Declared roles: facade

pub mod account;
pub mod discovery;
pub mod dispatch;
pub mod encoding;
pub mod envelope;
pub mod launch;
pub mod opencode;
pub mod policy;
pub mod schema;
pub mod session;
pub mod terminal;

pub use dispatch::handle_invocation;
pub use dispatch::write_invocation;
