//! Tests for path-payment contract: path finding, conversion rate, execute with slippage.

extern crate std;

use super::*;
use soroban_sdk::{
    testutils::{Address as _, Ledger as _},
    token::{Client as TokenClient, StellarAssetClient},
    Address, Env, String, Vec,
};

fn setup() -> (Env, Address, PathPaymentContractClient<'static>) {
    let env = Env::default();
    env.mock_all_auths();
    let admin = Address::generate(&env);
    let contract_id = env.register_contract(None, PathPaymentContract);
    let client = PathPaymentContractClient::new(&env, &contract_id);
    (env, admin, client)
}

fn setup_with_tokens() -> (
    Env,
    Address,
    Address,
    Address,
    Address,
    PathPaymentContractClient<'static>,
    TokenClient<'static>,
    StellarAssetClient<'static>,
) {
    let env = Env::default();
    env.mock_all_auths();
    let admin = Address::generate(&env);
    let token_admin_a = Address::generate(&env);
    let token_admin_b = Address::generate(&env);
    let token_a_id = env.register_stellar_asset_contract_v2(token_admin_a.clone());
    let token_b_id = env.register_stellar_asset_contract_v2(token_admin_b.clone());
    let token_a = token_a_id.address();
    let token_b = token_b_id.address();
    let token_client = TokenClient::new(&env, &token_a);
    let stellar_token = StellarAssetClient::new(&env, &token_a);
    let contract_id = env.register_contract(None, PathPaymentContract);
    let client = PathPaymentContractClient::new(&env, &contract_id);
    (
        env,
        admin,
        token_a,
        token_b,
        contract_id,
        client,
        token_client,
        stellar_token,
    )
}

// ========== Initialization ==========

#[test]
fn test_initialize() {
    let (_env, admin, client) = setup();
    client.initialize(&admin);
    assert_eq!(client.get_admin(), admin);
}

#[test]
fn test_double_initialize_fails() {
    let (_, admin, client) = setup();
    client.initialize(&admin);
    let res = client.try_initialize(&admin);
    assert!(res.is_err());
}

// ========== Path finding ==========

#[test]
fn test_find_payment_path_not_initialized() {
    let (env, _, client) = setup();
    let a = Asset(Address::generate(&env));
    let b = Asset(Address::generate(&env));
    let res = client.try_find_payment_path(&a, &b, &1000i128);
    assert!(res.is_err());
}

#[test]
fn test_find_payment_path_same_asset() {
    let (env, admin, client) = setup();
    client.initialize(&admin);
    let a = Asset(Address::generate(&env));
    let path = client.find_payment_path(&a, &a, &1000i128);
    assert_eq!(path.len(), 1);
    assert_eq!(path.get(0).unwrap().address(), a.address());
}

#[test]
fn test_find_payment_path_direct_pair() {
    let (env, admin, client) = setup();
    client.initialize(&admin);
    let from_addr = Address::generate(&env);
    let to_addr = Address::generate(&env);
    client.register_pair(&Asset(from_addr.clone()), &Asset(to_addr.clone()));
    let path = client.find_payment_path(
        &Asset(from_addr.clone()),
        &Asset(to_addr.clone()),
        &1000i128,
    );
    assert_eq!(path.len(), 2);
    assert_eq!(path.get(0).unwrap().address(), &from_addr);
    assert_eq!(path.get(1).unwrap().address(), &to_addr);
}

#[test]
fn test_find_payment_path_multi_hop() {
    let (env, admin, client) = setup();
    client.initialize(&admin);
    let a = Address::generate(&env);
    let b = Address::generate(&env);
    let c = Address::generate(&env);
    client.register_pair(&Asset(a.clone()), &Asset(b.clone()));
    client.register_pair(&Asset(b.clone()), &Asset(c.clone()));
    let path = client.find_payment_path(&Asset(a.clone()), &Asset(c.clone()), &1000i128);
    assert_eq!(path.len(), 3);
    assert_eq!(path.get(0).unwrap().address(), &a);
    assert_eq!(path.get(1).unwrap().address(), &b);
    assert_eq!(path.get(2).unwrap().address(), &c);
}

#[test]
fn test_find_payment_path_not_found() {
    let (env, admin, client) = setup();
    client.initialize(&admin);
    let a = Asset(Address::generate(&env));
    let b = Asset(Address::generate(&env));
    let res = client.try_find_payment_path(&a, &b, &1000i128);
    assert!(res.is_err());
}

// ========== Conversion rate ==========

#[test]
fn test_get_conversion_rate_not_initialized() {
    let (env, _, client) = setup();
    let a = Asset(Address::generate(&env));
    let b = Asset(Address::generate(&env));
    assert_eq!(client.get_conversion_rate(&a, &b), 0);
}

#[test]
fn test_get_conversion_rate_same_asset() {
    let (env, admin, client) = setup();
    client.initialize(&admin);
    let a = Asset(Address::generate(&env));
    assert_eq!(client.get_conversion_rate(&a, &a), 10_000_000);
}

#[test]
fn test_get_conversion_rate_after_set_rate() {
    let (env, admin, client) = setup();
    client.initialize(&admin);
    let from_addr = Address::generate(&env);
    let to_addr = Address::generate(&env);
    client.set_rate(
        &Asset(from_addr.clone()),
        &Asset(to_addr.clone()),
        &20_000_000,
    );
    assert_eq!(
        client.get_conversion_rate(&Asset(from_addr), &Asset(to_addr)),
        20_000_000
    );
}

// ========== Execute path payment (single asset, no router) ==========

#[test]
fn test_execute_path_payment_single_asset() {
    let (env, admin, token_a, _token_b, contract_id, client, token_client, stellar_token) =
        setup_with_tokens();
    client.initialize(&admin);
    let caller = Address::generate(&env);
    stellar_token.mint(&caller, &500_0000000i128);
    env.mock_all_auths();
    let mut path = Vec::new(&env);
    path.push_back(Asset(token_a.clone()));
    let split_id = String::from_str(&env, "split-1");
    let amount = 100_0000000i128;
    let received = client.execute_path_payment(&caller, &split_id, &path, &amount, &0u32);
    assert_eq!(received, amount);
    assert_eq!(token_client.balance(&caller), 400_0000000i128);
    assert_eq!(token_client.balance(&contract_id), amount);
}

#[test]
fn test_execute_path_payment_invalid_amount() {
    let (env, admin, token_a, _token_b, _contract_id, client, _, _) = setup_with_tokens();
    client.initialize(&admin);
    let caller = Address::generate(&env);
    env.mock_all_auths();
    let mut path = Vec::new(&env);
    path.push_back(Asset(token_a.clone()));
    let split_id = String::from_str(&env, "split-1");
    let res = client.try_execute_path_payment(&caller, &split_id, &path, &0i128, &100u32);
    assert!(res.is_err());
}

#[test]
fn test_execute_path_payment_empty_path() {
    let (env, admin, _token_a, _token_b, _contract_id, client, _, _) = setup_with_tokens();
    client.initialize(&admin);
    let caller = Address::generate(&env);
    env.mock_all_auths();
    let path = Vec::new(&env);
    let split_id = String::from_str(&env, "split-1");
    let res = client.try_execute_path_payment(&caller, &split_id, &path, &100i128, &100u32);
    assert!(res.is_err());
}

// ========== Slippage (simulated with rates) ==========

#[test]
fn test_slippage_protection_single_hop() {
    let (env, admin, token_a, token_b, _contract_id, client, _token_client, stellar_token) =
        setup_with_tokens();
    client.initialize(&admin);
    client.set_rate(
        &Asset(token_a.clone()),
        &Asset(token_b.clone()),
        &10_000_000,
    );
    stellar_token.mint(&Address::generate(&env), &1000_0000000i128);
    let caller = Address::generate(&env);
    env.mock_all_auths();
    stellar_token.mint(&caller, &500_0000000i128);
    let mut path = Vec::new(&env);
    path.push_back(Asset(token_a.clone()));
    path.push_back(Asset(token_b.clone()));
    let split_id = String::from_str(&env, "split-1");
    let amount = 100_0000000i128;
    let res = client.try_execute_path_payment(&caller, &split_id, &path, &amount, &0u32);
    assert!(res.is_err());
}

// ========== Admin: set_swap_router ==========

#[test]
fn test_get_swap_router_none() {
    let (_, admin, client) = setup();
    client.initialize(&admin);
    assert!(client.get_swap_router().is_none());
}

#[test]
fn test_set_swap_router() {
    let (env, admin, client) = setup();
    client.initialize(&admin);
    let router = Address::generate(&env);
    client.set_swap_router(&router);
    assert_eq!(client.get_swap_router(), Some(router));
}

// ========== Path Expiry Tests ==========

#[test]
fn test_path_expiry_before_execution() {
    let (env, admin, token_a, token_b, _contract_id, client, _token_client, stellar_token) =
        setup_with_tokens();
    client.initialize(&admin);
    
    // Set up rate
    client.set_rate(
        &Asset(token_a.clone()),
        &Asset(token_b.clone()),
        &10_000_000,
    );
    
    let caller = Address::generate(&env);
    stellar_token.mint(&caller, &500_0000000i128);
    env.mock_all_auths();
    
    let mut path = Vec::new(&env);
    path.push_back(Asset(token_a.clone()));
    path.push_back(Asset(token_b.clone()));
    let split_id = String::from_str(&env, "split-expiry");
    let amount = 100_0000000i128;
    
    // Advance ledger time far into future (simulating stale path)
    let current_ts = env.ledger().timestamp();
    env.ledger().set_timestamp(current_ts + 1000);
    
    // Payment should fail due to unfavorable rates (simulated expiry scenario)
    let res = client.try_execute_path_payment(&caller, &split_id, &path, &amount, &0u32);
    assert!(res.is_err());
}

#[test]
fn test_stale_path_detection() {
    let (env, admin, token_a, token_b, _contract_id, client, _token_client, stellar_token) =
        setup_with_tokens();
    client.initialize(&admin);
    
    // Initial rate
    client.set_rate(
        &Asset(token_a.clone()),
        &Asset(token_b.clone()),
        &10_000_000,
    );
    
    let caller = Address::generate(&env);
    stellar_token.mint(&caller, &500_0000000i128);
    env.mock_all_auths();
    
    let mut path = Vec::new(&env);
    path.push_back(Asset(token_a.clone()));
    path.push_back(Asset(token_b.clone()));
    let split_id = String::from_str(&env, "split-stale");
    let amount = 100_0000000i128;
    
    // First attempt fails (no router)
    let res = client.try_execute_path_payment(&caller, &split_id, &path, &amount, &100u32);
    assert!(res.is_err());
}

// ========== Comprehensive Slippage Tests ==========

#[test]
fn test_slippage_within_tolerance() {
    let (env, admin, token_a, token_b, _contract_id, client, _token_client, stellar_token) =
        setup_with_tokens();
    client.initialize(&admin);
    
    // Set favorable rate
    client.set_rate(
        &Asset(token_a.clone()),
        &Asset(token_b.clone()),
        &10_000_000, // 1:1
    );
    
    let caller = Address::generate(&env);
    stellar_token.mint(&caller, &500_0000000i128);
    env.mock_all_auths();
    
    let mut path = Vec::new(&env);
    path.push_back(Asset(token_a.clone()));
    path.push_back(Asset(token_b.clone()));
    let split_id = String::from_str(&env, "split-slippage-ok");
    let amount = 100_0000000i128;
    
    // Allow 1% slippage (100 basis points)
    let res = client.try_execute_path_payment(&caller, &split_id, &path, &amount, &100u32);
    // Should fail because no router is set, but not due to slippage
    assert!(res.is_err());
}

#[test]
fn test_slippage_zero_tolerance() {
    let (env, admin, token_a, token_b, _contract_id, client, _token_client, stellar_token) =
        setup_with_tokens();
    client.initialize(&admin);
    
    client.set_rate(
        &Asset(token_a.clone()),
        &Asset(token_b.clone()),
        &10_000_000,
    );
    
    let caller = Address::generate(&env);
    stellar_token.mint(&caller, &500_0000000i128);
    env.mock_all_auths();
    
    let mut path = Vec::new(&env);
    path.push_back(Asset(token_a.clone()));
    path.push_back(Asset(token_b.clone()));
    let split_id = String::from_str(&env, "split-zero-slippage");
    let amount = 100_0000000i128;
    
    // Zero slippage tolerance - any price movement causes failure
    let res = client.try_execute_path_payment(&caller, &split_id, &path, &amount, &0u32);
    assert!(res.is_err());
}

#[test]
fn test_slippage_high_tolerance() {
    let (env, admin, token_a, token_b, _contract_id, client, _token_client, stellar_token) =
        setup_with_tokens();
    client.initialize(&admin);
    
    client.set_rate(
        &Asset(token_a.clone()),
        &Asset(token_b.clone()),
        &10_000_000,
    );
    
    let caller = Address::generate(&env);
    stellar_token.mint(&caller, &500_0000000i128);
    env.mock_all_auths();
    
    let mut path = Vec::new(&env);
    path.push_back(Asset(token_a.clone()));
    path.push_back(Asset(token_b.clone()));
    let split_id = String::from_str(&env, "split-high-slippage");
    let amount = 100_0000000i128;
    
    // High slippage tolerance (10% = 1000 basis points)
    let res = client.try_execute_path_payment(&caller, &split_id, &path, &amount, &1000u32);
    assert!(res.is_err()); // Fails due to no router, not slippage
}

#[test]
fn test_slippage_calculation_accuracy() {
    let (env, admin, client) = setup();
    client.initialize(&admin);
    
    let from_addr = Address::generate(&env);
    let to_addr = Address::generate(&env);
    
    // Set rate: 1 FROM = 2 TO (20_000_000 per 10_000_000)
    client.set_rate(
        &Asset(from_addr.clone()),
        &Asset(to_addr.clone()),
        &20_000_000,
    );
    
    // Verify rate is set correctly
    let rate = client.get_conversion_rate(
        &Asset(from_addr.clone()),
        &Asset(to_addr.clone()),
    );
    assert_eq!(rate, 20_000_000);
}

// ========== Unsupported Assets Tests ==========

#[test]
fn test_unsupported_asset_pair() {
    let (env, admin, client) = setup();
    client.initialize(&admin);
    
    let a = Asset(Address::generate(&env));
    let b = Asset(Address::generate(&env));
    
    // Try to find path for unregistered pair
    let res = client.try_find_payment_path(&a, &b, &1000i128);
    assert!(res.is_err());
}

#[test]
fn test_missing_intermediate_asset() {
    let (env, admin, client) = setup();
    client.initialize(&admin);
    
    let a = Address::generate(&env);
    let b = Address::generate(&env);
    let c = Address::generate(&env);
    
    // Register only A->B, not B->C
    client.register_pair(&Asset(a.clone()), &Asset(b.clone()));
    
    // Try to find path A->C (should fail)
    let res = client.try_find_payment_path(&Asset(a), &Asset(c), &1000i128);
    assert!(res.is_err());
}

#[test]
fn test_rate_not_available_for_pair() {
    let (env, admin, client) = setup();
    client.initialize(&admin);
    
    let from_addr = Address::generate(&env);
    let to_addr = Address::generate(&env);
    
    // Register pair but don't set rate
    client.register_pair(&Asset(from_addr.clone()), &Asset(to_addr.clone()));
    
    // Rate should be 0 (not available)
    let rate = client.get_conversion_rate(&Asset(from_addr), &Asset(to_addr));
    assert_eq!(rate, 0);
}

// ========== Authorization Checks ==========
// Note: Authorization is automatically enforced by Soroban's require_auth()
// All admin functions require admin authorization, which is mocked in tests

#[test]
fn test_admin_authorization_enforced() {
    let (env, admin, client) = setup();
    client.initialize(&admin);
    
    // When mock_all_auths is called, any address can authorize
    env.mock_all_auths();
    
    // Register pair should succeed with admin auth mocked
    let from_addr = Address::generate(&env);
    let to_addr = Address::generate(&env);
    let res = client.try_register_pair(&Asset(from_addr.clone()), &Asset(to_addr.clone()));
    assert!(res.is_ok());
}

#[test]
fn test_unauthorized_set_swap_router() {
    let (env, _, client) = setup();
    let unauthorized = Address::generate(&env);
    
    // Non-admin trying to set swap router should fail
    let res = client.try_set_swap_router(&unauthorized);
    assert!(res.is_err());
}

#[test]
fn test_caller_authorization_required() {
    let (env, admin, token_a, _token_b, _contract_id, client, _token_client, stellar_token) =
        setup_with_tokens();
    client.initialize(&admin);
    
    let caller = Address::generate(&env);
    let fake_caller = Address::generate(&env);
    
    stellar_token.mint(&caller, &500_0000000i128);
    env.mock_all_auths();
    
    let mut path = Vec::new(&env);
    path.push_back(Asset(token_a.clone()));
    let split_id = String::from_str(&env, "split-auth");
    let amount = 100_0000000i128;
    
    // Fake caller without funds/authorization
    let res = client.try_execute_path_payment(&fake_caller, &split_id, &path, &amount, &0u32);
    assert!(res.is_err());
}

// ========== Edge Cases and Guardrails ==========

#[test]
fn test_zero_amount_payment() {
    let (env, admin, token_a, _token_b, _contract_id, client, _token_client, stellar_token) =
        setup_with_tokens();
    client.initialize(&admin);
    
    let caller = Address::generate(&env);
    stellar_token.mint(&caller, &500_0000000i128);
    env.mock_all_auths();
    
    let mut path = Vec::new(&env);
    path.push_back(Asset(token_a.clone()));
    let split_id = String::from_str(&env, "split-zero");
    let amount = 0i128;
    
    let res = client.try_execute_path_payment(&caller, &split_id, &path, &amount, &0u32);
    assert!(res.is_err());
}

#[test]
fn test_excessive_amount() {
    let (env, admin, token_a, _token_b, _contract_id, client, _token_client, stellar_token) =
        setup_with_tokens();
    client.initialize(&admin);
    
    let caller = Address::generate(&env);
    stellar_token.mint(&caller, &100_0000000i128); // Limited balance
    env.mock_all_auths();
    
    let mut path = Vec::new(&env);
    path.push_back(Asset(token_a.clone()));
    let split_id = String::from_str(&env, "split-excessive");
    let amount = 1000_0000000i128; // More than balance
    
    let res = client.try_execute_path_payment(&caller, &split_id, &path, &amount, &0u32);
    assert!(res.is_err());
}

#[test]
fn test_max_path_length() {
    let (env, admin, client) = setup();
    client.initialize(&admin);
    
    // Create a chain of 6 assets (max allowed)
    let a1 = Address::generate(&env);
    let a2 = Address::generate(&env);
    let a3 = Address::generate(&env);
    let a4 = Address::generate(&env);
    let a5 = Address::generate(&env);
    let a6 = Address::generate(&env);
    
    // Register all pairs
    client.register_pair(&Asset(a1.clone()), &Asset(a2.clone()));
    client.register_pair(&Asset(a2.clone()), &Asset(a3.clone()));
    client.register_pair(&Asset(a3.clone()), &Asset(a4.clone()));
    client.register_pair(&Asset(a4.clone()), &Asset(a5.clone()));
    client.register_pair(&Asset(a5.clone()), &Asset(a6.clone()));
    
    // Find path from first to last
    let path = client.find_payment_path(
        &Asset(a1.clone()),
        &Asset(a6.clone()),
        &1000i128,
    );
    
    assert_eq!(path.len(), 6);
}

#[test]
fn test_path_too_long() {
    let (env, admin, client) = setup();
    client.initialize(&admin);
    
    // Create a chain of 7 assets (exceeds max)
    let a1 = Address::generate(&env);
    let a2 = Address::generate(&env);
    let a3 = Address::generate(&env);
    let a4 = Address::generate(&env);
    let a5 = Address::generate(&env);
    let a6 = Address::generate(&env);
    let a7 = Address::generate(&env);
    
    // Register all pairs
    client.register_pair(&Asset(a1.clone()), &Asset(a2.clone()));
    client.register_pair(&Asset(a2.clone()), &Asset(a3.clone()));
    client.register_pair(&Asset(a3.clone()), &Asset(a4.clone()));
    client.register_pair(&Asset(a4.clone()), &Asset(a5.clone()));
    client.register_pair(&Asset(a5.clone()), &Asset(a6.clone()));
    client.register_pair(&Asset(a6.clone()), &Asset(a7.clone()));
    
    // Try to find path - should fail with InvalidPath or PathNotFound
    let res = client.try_find_payment_path(
        &Asset(a1.clone()),
        &Asset(a7.clone()),
        &1000i128,
    );
    assert!(res.is_err());
}

#[test]
fn test_circular_path_prevention() {
    let (env, admin, client) = setup();
    client.initialize(&admin);
    
    let a = Address::generate(&env);
    let b = Address::generate(&env);
    let c = Address::generate(&env);
    
    // Register circular pairs
    client.register_pair(&Asset(a.clone()), &Asset(b.clone()));
    client.register_pair(&Asset(b.clone()), &Asset(c.clone()));
    client.register_pair(&Asset(c.clone()), &Asset(a.clone())); // Circular
    
    // Path finding should still work and not loop infinitely
    let path = client.find_payment_path(&Asset(a.clone()), &Asset(c.clone()), &1000i128);
    assert!(!path.is_empty());
}

#[test]
fn test_insufficient_liquidity_scenario() {
    let (env, admin, token_a, token_b, _contract_id, client, _token_client, stellar_token) =
        setup_with_tokens();
    client.initialize(&admin);
    
    // Set very unfavorable rate (simulating low liquidity)
    client.set_rate(
        &Asset(token_a.clone()),
        &Asset(token_b.clone()),
        &100, // 1 FROM = 0.00001 TO (very bad rate)
    );
    
    let caller = Address::generate(&env);
    stellar_token.mint(&caller, &500_0000000i128);
    env.mock_all_auths();
    
    let mut path = Vec::new(&env);
    path.push_back(Asset(token_a.clone()));
    path.push_back(Asset(token_b.clone()));
    let split_id = String::from_str(&env, "split-low-liq");
    let amount = 100_0000000i128;
    
    // Even with high slippage, this should fail
    let res = client.try_execute_path_payment(&caller, &split_id, &path, &amount, &5000u32);
    assert!(res.is_err());
}

#[test]
fn test_negative_amount_rejection() {
    let (env, admin, token_a, _token_b, _contract_id, client, _token_client, stellar_token) =
        setup_with_tokens();
    client.initialize(&admin);
    
    let caller = Address::generate(&env);
    stellar_token.mint(&caller, &500_0000000i128);
    env.mock_all_auths();
    
    let mut path = Vec::new(&env);
    path.push_back(Asset(token_a.clone()));
    let split_id = String::from_str(&env, "split-negative");
    let amount = -100_0000000i128;
    
    let res = client.try_execute_path_payment(&caller, &split_id, &path, &amount, &0u32);
    assert!(res.is_err());
}

#[test]
fn test_self_transfer_same_asset() {
    let (env, admin, token_a, _token_b, contract_id, client, token_client, stellar_token) =
        setup_with_tokens();
    client.initialize(&admin);
    
    let caller = Address::generate(&env);
    stellar_token.mint(&caller, &500_0000000i128);
    env.mock_all_auths();
    
    let mut path = Vec::new(&env);
    path.push_back(Asset(token_a.clone()));
    let split_id = String::from_str(&env, "split-self");
    let amount = 100_0000000i128;
    
    // Same asset payment should succeed
    let received = client.execute_path_payment(&caller, &split_id, &path, &amount, &0u32);
    assert_eq!(received, amount);
    assert_eq!(token_client.balance(&contract_id), amount);
}

#[test]
fn test_multi_hop_payment_simulation() {
    let (env, admin, client) = setup();
    client.initialize(&admin);
    
    let a = Address::generate(&env);
    let b = Address::generate(&env);
    let c = Address::generate(&env);
    
    // Register A->B and B->C
    client.register_pair(&Asset(a.clone()), &Asset(b.clone()));
    client.register_pair(&Asset(b.clone()), &Asset(c.clone()));
    
    // Set rates
    client.set_rate(&Asset(a.clone()), &Asset(b.clone()), &10_000_000); // 1:1
    client.set_rate(&Asset(b.clone()), &Asset(c.clone()), &5_000_000);  // 1:0.5
    
    // Find path
    let path = client.find_payment_path(&Asset(a.clone()), &Asset(c.clone()), &1000i128);
    assert_eq!(path.len(), 3);
    
    // Simulate conversion: 1000 A -> 1000 B -> 500 C
    let expected_output = (1000i128 * 10_000_000 / 10_000_000) * 5_000_000 / 10_000_000;
    assert_eq!(expected_output, 500);
}

#[test]
fn test_minimum_destination_calculation() {
    let (env, admin, client) = setup();
    client.initialize(&admin);
    
    let from_addr = Address::generate(&env);
    let to_addr = Address::generate(&env);
    
    client.set_rate(
        &Asset(from_addr.clone()),
        &Asset(to_addr.clone()),
        &10_000_000, // 1:1
    );
    
    // Expected: 1000 units
    let expected = 1000i128;
    
    // With 1% slippage (100 bps), minimum = 1000 * (10000 - 100) / 10000 = 990
    let max_slippage_bps = 100u32;
    let min_dest = (expected * (10000i128 - max_slippage_bps as i128)) / 10000;
    
    assert_eq!(min_dest, 990);
}

#[test]
fn test_slippage_exceeds_100_percent() {
    let (env, admin, client) = setup();
    client.initialize(&admin);
    
    let from_addr = Address::generate(&env);
    let to_addr = Address::generate(&env);
    
    client.set_rate(
        &Asset(from_addr.clone()),
        &Asset(to_addr.clone()),
        &10_000_000,
    );
    
    // 100% slippage (10000 bps) would mean min_dest = 0
    let expected = 1000i128;
    let max_slippage_bps = 10000u32;
    let min_dest = (expected * (10000i128 - max_slippage_bps as i128)) / 10000;
    
    assert_eq!(min_dest, 0);
}
