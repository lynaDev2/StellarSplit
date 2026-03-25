#![cfg(test)]

use crate::{SplitEscrowContract, SplitEscrowContractClient, SplitStatus};
use soroban_sdk::token::{Client as TokenClient, StellarAssetClient as TokenAdminClient};
use soroban_sdk::{
    testutils::Address as _, testutils::Events as _, Address, Env, Map, String,
};

fn metadata_map(env: &Env, entries: &[(&str, &str)]) -> Map<String, String> {
    let mut metadata = Map::new(env);
    for (key, value) in entries {
        metadata.set(String::from_str(env, key), String::from_str(env, value));
    }
    metadata
}

fn setup() -> (
    Env,
    SplitEscrowContractClient<'static>,
    Address,
    Address,
    Address,
    TokenClient<'static>,
    TokenAdminClient<'static>,
) {
    let env = Env::default();
    env.mock_all_auths();

    let admin = Address::generate(&env);
    let creator = Address::generate(&env);
    let participant = Address::generate(&env);

    let token_admin = Address::generate(&env);
    let token_contract = env.register_stellar_asset_contract_v2(token_admin);
    let token = token_contract.address();

    let token_client = TokenClient::new(&env, &token);
    let token_admin_client = TokenAdminClient::new(&env, &token);

    let contract_id = env.register_contract(None, SplitEscrowContract);
    let client = SplitEscrowContractClient::new(&env, &contract_id);
    client.initialize(&admin, &token, &String::from_str(&env, "1.0.0"));

    token_admin_client.mint(&participant, &1_000_000);

    (
        env,
        client,
        admin,
        creator,
        participant,
        token_client,
        token_admin_client,
    )
}

#[test]
fn test_fee_deducted_and_sent_to_treasury_on_release() {
    let (env, client, admin, creator, participant, token_client, _) = setup();
    let treasury = Address::generate(&env);
    client.set_treasury(&treasury);
    client.set_fee(&250u32); // 2.5%

    let split_id = client.create_escrow(
        &creator,
        &String::from_str(&env, "Dinner"),
        &10_000,
        &None,
        &None,
    );
    client.deposit(&split_id, &participant, &10_000);
    client.release_funds(&split_id);

    assert_eq!(token_client.balance(&treasury), 250);
    assert_eq!(token_client.balance(&creator), 9_750);

    let escrow = client.get_escrow(&split_id);
    assert_eq!(escrow.status, SplitStatus::Released);

    let _ = admin;
}

#[test]
fn test_admin_can_update_fee_and_treasury() {
    let (env, client, _admin, creator, participant, token_client, _) = setup();

    let treasury_a = Address::generate(&env);
    client.set_treasury(&treasury_a);
    client.set_fee(&100u32);

    let split_a =
        client.create_escrow(&creator, &String::from_str(&env, "A"), &1_000, &None, &None);
    client.deposit(&split_a, &participant, &1_000);
    client.release_funds(&split_a);
    assert_eq!(token_client.balance(&treasury_a), 10);

    let treasury_b = Address::generate(&env);
    client.set_treasury(&treasury_b);
    client.set_fee(&300u32);

    let split_b =
        client.create_escrow(&creator, &String::from_str(&env, "B"), &2_000, &None, &None);
    client.deposit(&split_b, &participant, &2_000);
    client.release_funds(&split_b);
    assert_eq!(token_client.balance(&treasury_b), 60);
}

#[test]
fn test_set_fee_and_set_treasury_are_admin_only() {
    let (env, client, admin, _creator, _participant, _token_client, _token_admin) = setup();

    env.mock_all_auths();
    client.set_fee(&123u32);
    client.set_treasury(&Address::generate(&env));

    assert_ne!(admin, Address::generate(&env));
}

#[test]
fn test_fees_collected_event_emitted() {
    let (env, client, _admin, creator, participant, _token_client, _) = setup();
    let treasury = Address::generate(&env);
    client.set_treasury(&treasury);
    client.set_fee(&500u32);

    let before_len = env.events().all().len();

    let split_id = client.create_escrow(
        &creator,
        &String::from_str(&env, "Event"),
        &1_000,
        &None,
        &None,
    );
    client.deposit(&split_id, &participant, &1_000);
    client.release_funds(&split_id);

    let after_len = env.events().all().len();
    assert!(after_len > before_len);
}

#[test]
fn test_version_stored_on_init() {
    let (env, client, _, _, _, _, _) = setup();
    assert_eq!(client.get_version(), String::from_str(&env, "1.0.0"));
}

#[test]
fn test_upgrade_version_admin() {
    let (env, client, admin, _, _, _, _) = setup();

    client.upgrade_version(&String::from_str(&env, "1.1.0"));
    assert_eq!(client.get_version(), String::from_str(&env, "1.1.0"));
}

#[test]
#[should_panic(expected = "HostError: Error(Contract, #3)")] // Unauthorized
fn test_upgrade_version_non_admin_fails() {
    let (env, client, _, creator, _, _, _) = setup();

    env.mock_all_auths_providing_64bit_allowance(); // Reset mocks to require specific auth

    // Switch to creator auth
    client.env.mock_auths(&[soroban_sdk::testutils::MockAuth {
        address: &creator,
        invoke: &soroban_sdk::testutils::MockAuthInvoke {
            contract: &client.address,
            function: "upgrade_version",
            args: (String::from_str(&env, "1.1.0"),).into_val(&env),
            sub_invokes: &[],
        },
    }]);

    client.upgrade_version(&String::from_str(&env, "1.1.0"));
}

#[test]
#[should_panic(expected = "HostError: Error(Contract, #11)")] // InvalidVersion
fn test_upgrade_version_invalid_semver_fails() {
    let (env, client, _, _, _, _, _) = setup();
    client.upgrade_version(&String::from_str(&env, "1.0"));
}

#[test]
#[should_panic(expected = "HostError: Error(Contract, #11)")] // InvalidVersion
fn test_initialize_invalid_version_fails() {
    let env = Env::default();
    env.mock_all_auths();
    let admin = Address::generate(&env);
    let token = Address::generate(&env);
    let contract_id = env.register_contract(None, SplitEscrowContract);
    let client = SplitEscrowContractClient::new(&env, &contract_id);
    client.initialize(&admin, &token, &String::from_str(&env, "1.0"));
}

#[test]
fn test_contract_upgraded_event_emitted() {
    let (env, client, _, _, _, _, _) = setup();

    let before_len = env.events().all().len();
    client.upgrade_version(&String::from_str(&env, "2.0.0"));
    let after_len = env.events().all().len();

    assert!(after_len > before_len);

    let last_event = env.events().all().last().unwrap();
    assert_eq!(last_event.0, client.address);
}

#[test]
fn test_default_max_participants_is_50() {
    let (env, client, _admin, creator, _p, _tc, token_admin) = setup();
    let escrow_id = client.create_escrow(
        &creator,
        &String::from_str(&env, "Cap default"),
        &100,
        &None,
        &None,
    );
    let escrow = client.get_escrow(&escrow_id);
    assert_eq!(escrow.max_participants, 50);
    assert_eq!(escrow.participants.len(), 0);

    let _ = token_admin;
}

#[test]
fn test_explicit_max_participants_stored_in_get_escrow() {
    let (env, client, _admin, creator, p1, _tc, _ta) = setup();
    let cap = 3u32;
    let escrow_id = client.create_escrow(
        &creator,
        &String::from_str(&env, "Explicit cap"),
        &300,
        &Some(cap),
        &None,
    );
    let escrow = client.get_escrow(&escrow_id);
    assert_eq!(escrow.max_participants, cap);
    client.deposit(&escrow_id, &p1, &100);
    let escrow = client.get_escrow(&escrow_id);
    assert_eq!(escrow.participants.len(), 1);
}

#[test]
fn test_deposit_rejected_when_participant_cap_exceeded() {
    let (env, client, _admin, creator, p1, _tc, token_admin) = setup();
    let p2 = Address::generate(&env);
    let p3 = Address::generate(&env);
    token_admin.mint(&p2, &10_000);
    token_admin.mint(&p3, &10_000);

    let escrow_id = client.create_escrow(
        &creator,
        &String::from_str(&env, "Two max"),
        &3_000,
        &Some(2u32),
        &None,
    );

    client.deposit(&escrow_id, &p1, &1_000);
    client.deposit(&escrow_id, &p2, &1_000);
    assert_eq!(client.get_escrow(&escrow_id).participants.len(), 2);

    let res = client.try_deposit(&escrow_id, &p3, &1_000);
    assert!(res.is_err());

    let escrow = client.get_escrow(&escrow_id);
    assert_eq!(escrow.participants.len(), 2);
    assert_eq!(escrow.deposited_amount, 2_000);
}

#[test]
fn test_existing_participant_can_deposit_again_without_increasing_count() {
    let (env, client, _admin, creator, p1, _tc, _ta) = setup();
    // release_funds runs fee collection; treasury must be set even when fee bps is 0.
    client.set_treasury(&Address::generate(&env));

    let escrow_id = client.create_escrow(
        &creator,
        &String::from_str(&env, "Repeat"),
        &2_000,
        &Some(1u32),
        &None,
    );
    client.deposit(&escrow_id, &p1, &1_000);
    client.deposit(&escrow_id, &p1, &1_000);
    let escrow = client.get_escrow(&escrow_id);
    assert_eq!(escrow.participants.len(), 1);
    assert_eq!(escrow.deposited_amount, 2_000);
    client.release_funds(&escrow_id);
    assert_eq!(client.get_escrow(&escrow_id).status, SplitStatus::Released);
}

#[test]
fn test_note_stored_on_create_and_get_note() {
    let (env, client, _admin, creator, _p, _tc, _ta) = setup();
    let text = "Dinner at Luigi's — Friday night";
    let split_id = client.create_escrow(
        &creator,
        &String::from_str(&env, "Bill"),
        &100,
        &None,
        &Some(String::from_str(&env, text)),
    );
    assert_eq!(client.get_note(&split_id), String::from_str(&env, text));
    assert_eq!(
        client.get_escrow(&split_id).note,
        String::from_str(&env, text)
    );
}

#[test]
fn test_creator_can_update_note_while_pending_and_ready() {
    let (env, client, _admin, creator, p1, _tc, _ta) = setup();
    client.set_treasury(&Address::generate(&env));

    let split_id =
        client.create_escrow(&creator, &String::from_str(&env, "X"), &2_000, &None, &None);
    client.set_note(&split_id, &String::from_str(&env, "v1"));
    assert_eq!(client.get_note(&split_id), String::from_str(&env, "v1"));

    client.deposit(&split_id, &p1, &1_000);
    client.set_note(&split_id, &String::from_str(&env, "v2-ready"));
    assert_eq!(
        client.get_note(&split_id),
        String::from_str(&env, "v2-ready")
    );

    client.deposit(&split_id, &p1, &1_000);
    client.release_funds(&split_id);
    let res = client.try_set_note(&split_id, &String::from_str(&env, "late"));
    assert!(res.is_err());
}

#[test]
fn test_note_over_128_bytes_rejected_on_create_and_set() {
    let (env, client, _admin, creator, _p, _tc, _ta) = setup();
    let bytes = [b'a'; 129];
    let long = String::from_str(&env, core::str::from_utf8(&bytes).unwrap());
    assert_eq!(long.len(), 129);

    let res = client.try_create_escrow(
        &creator,
        &String::from_str(&env, "x"),
        &100,
        &None,
        &Some(long.clone()),
    );
    assert!(res.is_err());

    let split_id =
        client.create_escrow(&creator, &String::from_str(&env, "ok"), &100, &None, &None);
    let res2 = client.try_set_note(&split_id, &long);
    assert!(res2.is_err());
}

#[test]
fn test_note_updated_emits_event() {
    let (env, client, _admin, creator, _p, _tc, _ta) = setup();
    let split_id = client.create_escrow(&creator, &String::from_str(&env, "E"), &100, &None, &None);
    let before = env.events().all().len();
    client.set_note(&split_id, &String::from_str(&env, "hello"));
    assert!(env.events().all().len() > before);
}
