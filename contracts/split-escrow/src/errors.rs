use soroban_sdk::contracterror;

#[contracterror]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Error {
    AlreadyInitialized = 1,
    NotInitialized = 2,
    Unauthorized = 3,
    InvalidAmount = 4,
    InvalidFeeBps = 5,
    SplitNotFound = 6,
    SplitNotPending = 7,
    SplitNotReady = 8,
    TreasuryNotSet = 9,

    /// The maximum number of participants for this escrow has been reached.
    ParticipantCapExceeded = 10,

    InvalidVersion = 11,
    InvalidInput = 12,
    EscrowNotActive = 13,
    SplitNotActive = 14,
    InvalidMetadata = 15,
    ParticipantNotOwed = 16,
    InsufficientFulfillment = 17,
    TotalAmountMismatch = 18,
}
