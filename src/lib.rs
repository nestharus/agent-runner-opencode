//! Declared roles: accessor

pub mod account;
pub mod activity;
mod child_custody;
pub mod discovery;
pub mod dispatch;
pub mod encoding;
pub mod envelope;
pub mod launch;
pub mod migration;
pub mod models;
pub mod opencode;
pub mod path_guard;
pub mod policy;
pub mod quota;
pub mod quota_adapter;
pub mod resume_observation;
pub mod rotation;
pub mod runtime_selection;
pub mod schema;
pub mod session;
pub mod settings;
pub mod settings_definition;
pub mod setup;
pub mod shell;
pub mod terminal;

pub use dispatch::handle_invocation;
pub use dispatch::write_invocation;
