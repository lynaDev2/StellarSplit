//! Types for path-payment contract: Asset and errors.

use soroban_sdk::{contracterror, contracttype, Address};

/// Stellar asset represented as a Soroban token contract address.
#[contracttype]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Asset(pub Address);

impl Asset {
    pub fn address(&self) -> &Address {
        &self.0
    }
}

/// A directed edge for path finding.
#[contracttype]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssetPair {
    pub from: Address,
    pub to: Address,
}

/// Errors for path payment operations.
#[contracterror]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum Error {
    PathNotFound = 1,
    InvalidPath = 2,
    SlippageExceeded = 3,
    SwapFailed = 4,
    Unauthorized = 5,
    InvalidAmount = 6,
    SplitNotFound = 7,
    NotInitialized = 8,
    PairNotRegistered = 9,
    RateNotAvailable = 10,
    PathExpired = 11,
    UnsupportedAsset = 12,
    AmountTooLow = 13,
    AmountTooHigh = 14,
}
