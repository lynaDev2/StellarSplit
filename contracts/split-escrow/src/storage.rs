use soroban_sdk::{contracttype, Address, Env};

use crate::types::Split;

#[contracttype]
#[derive(Clone)]
pub enum DataKey {
    Admin,
    Token,
    NextSplitId,
    Split(u64),
    WhitelistEnabled(u64),
    WhitelistMember(u64, Address),
    FeeBps,
    Treasury,
    Version,
}

pub fn set_version(env: &Env, version: &String) {
    env.storage().instance().set(&DataKey::Version, version);
}

pub fn get_version(env: &Env) -> String {
    env.storage().instance().get(&DataKey::Version).unwrap()
}

pub fn has_admin(env: &Env) -> bool {
    env.storage().instance().has(&DataKey::Admin)
}

pub fn set_admin(env: &Env, admin: &Address) {
    env.storage().instance().set(&DataKey::Admin, admin);
}

pub fn get_admin(env: &Env) -> Address {
    env.storage().instance().get(&DataKey::Admin).unwrap()
}

pub fn set_token(env: &Env, token: &Address) {
    env.storage().instance().set(&DataKey::Token, token);
}

pub fn get_token(env: &Env) -> Address {
    env.storage().instance().get(&DataKey::Token).unwrap()
}

pub fn get_next_split_id(env: &Env) -> u64 {
    env.storage()
        .instance()
        .get(&DataKey::NextSplitId)
        .unwrap_or(1u64)
}

pub fn bump_next_split_id(env: &Env) {
    let next = get_next_split_id(env);
    env.storage()
        .instance()
        .set(&DataKey::NextSplitId, &(next + 1));
}

pub fn set_split(env: &Env, split: &Split) {
    let key = DataKey::Split(split.split_id);
    let max_ttl = env.ledger().max_ttl();
    env.storage().persistent().set(&key, split);
    env.storage().persistent().extend_ttl(&key, max_ttl, max_ttl);
}

pub fn get_split(env: &Env, split_id: u64) -> Option<Split> {
    env.storage().persistent().get(&DataKey::Split(split_id))
}

pub fn set_whitelist_enabled(env: &Env, split_id: u64, enabled: bool) {
    let key = DataKey::WhitelistEnabled(split_id);
    let max_ttl = env.ledger().max_ttl();
    env.storage().persistent().set(&key, &enabled);
    env.storage().persistent().extend_ttl(&key, max_ttl, max_ttl);
}

pub fn is_whitelist_enabled(env: &Env, split_id: u64) -> bool {
    env.storage()
        .persistent()
        .get(&DataKey::WhitelistEnabled(split_id))
        .unwrap_or(false)
}

pub fn add_to_whitelist(env: &Env, split_id: u64, address: &Address) {
    let key = DataKey::WhitelistMember(split_id, address.clone());
    let max_ttl = env.ledger().max_ttl();
    env.storage().persistent().set(&key, &true);
    env.storage().persistent().extend_ttl(&key, max_ttl, max_ttl);
}

pub fn remove_from_whitelist(env: &Env, split_id: u64, address: &Address) {
    env.storage()
        .persistent()
        .remove(&DataKey::WhitelistMember(split_id, address.clone()));
}

pub fn is_whitelisted(env: &Env, split_id: u64, address: &Address) -> bool {
    env.storage()
        .persistent()
        .has(&DataKey::WhitelistMember(split_id, address.clone()))
}

pub fn set_fee_bps(env: &Env, fee_bps: u32) {
    env.storage().instance().set(&DataKey::FeeBps, &fee_bps);
}

pub fn get_fee_bps(env: &Env) -> u32 {
    env.storage()
        .instance()
        .get(&DataKey::FeeBps)
        .unwrap_or(0u32)
}

pub fn set_treasury(env: &Env, treasury: &Address) {
    env.storage().instance().set(&DataKey::Treasury, treasury);
}

pub fn get_treasury(env: &Env) -> Option<Address> {
    env.storage().instance().get(&DataKey::Treasury)
}
