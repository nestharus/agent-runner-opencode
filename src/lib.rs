//! Declared roles: facade

pub mod account;
pub mod codex;
pub mod discovery;
pub mod dispatch;
pub mod encoding;
pub mod envelope;
pub mod launch;
pub mod migration;
pub mod models;
pub mod opencode;
pub mod policy;
pub mod quota;
pub mod rotation;
pub mod schema;
pub mod session;
pub mod settings;
pub mod setup;
pub mod shell;
pub mod terminal;

pub use dispatch::handle_invocation;
pub use dispatch::write_invocation;
