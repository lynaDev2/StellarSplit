use soroban_sdk::{contracttype, Address, Map, String, Vec};

#[contracttype]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SplitStatus {
    Pending,
    Ready,
    Released,
}

/// Escrow split state. `participants.len()` is the current distinct participant count.
#[contracttype]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Split {
    pub split_id: u64,
    pub creator: Address,
    pub description: String,
    pub metadata: Map<String, String>,
    pub total_amount: i128,
    pub deposited_amount: i128,
    pub status: SplitStatus,
    /// Maximum distinct participants allowed (default 50 at creation if not specified).
    pub max_participants: u32,
    /// Distinct addresses that have deposited; length is the current participant count.
    pub participants: Vec<Address>,
}
