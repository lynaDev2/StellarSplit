//! # Events Module for Split Template Contract
//!
//! Defines contract events emitted by the template management functions.

use soroban_sdk::{Address, Env, String, Symbol};

/// Emit an event when a template is created (includes schema version).
pub fn emit_template_created(
    env: &Env,
    template_id: String,
    creator: Address,
    name: String,
    version: u32,
) {
    env.events().publish(
        (Symbol::new(env, "template_created"), template_id),
        (creator, name, version),
    );
}

/// Emit an event when a template is used to create a split.
pub fn emit_template_used(env: &Env, template_id: String, split_id: u64) {
    env.events()
        .publish((Symbol::new(env, "template_used"), template_id), split_id);
}
