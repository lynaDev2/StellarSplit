use soroban_sdk::{Address, Env, String, Symbol};

use crate::types::Split;

pub fn emit_initialized(env: &Env, admin: &Address) {
    env.events().publish(("init", "admin"), admin.clone());
}

pub fn emit_split_created(env: &Env, split: &Split) {
    env.events().publish(
        ("created", "split_id", "creator"),
        (split.split_id, split.creator.clone()),
    );
}

pub fn emit_deposit(env: &Env, split_id: u64, participant: &Address, amount: i128) {
    env.events().publish(
        ("deposit", "split_id", "participant"),
        (split_id, participant.clone(), amount),
    );
}

pub fn emit_released(env: &Env, split_id: u64, released_amount: i128) {
    env.events()
        .publish(("released", "split_id"), (split_id, released_amount));
}

pub fn emit_cancelled(env: &Env, split_id: u64) {
    env.events().publish(("cancelled", "split_id"), split_id);
}

pub fn emit_fees_collected(env: &Env, amount: i128, treasury: &Address) {
    env.events().publish(
        (Symbol::new(env, "FeesCollected"),),
        (amount, treasury.clone()),
    );
}

pub fn emit_note_updated(env: &Env, split_id: u64, note: &String) {
    env.events()
        .publish(("NoteUpdated", "split_id"), (split_id, note.clone()));
}
