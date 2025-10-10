/// Test demonstrating the race condition fix
///
/// This test simulates concurrent payment processing that would trigger
/// the "Payment ID already exists" error before the fix.
use cdk_common::mint::MintQuote;
use cdk_common::Amount;
use cdk_sql_common::SqlMintTransaction;
use std::sync::Arc;
use tokio::task::JoinSet;

#[tokio::test]
async fn test_concurrent_payment_id_insertion() {
    // Setup: Create a mint quote
    let db = setup_test_database().await;

    let quote = MintQuote::new(
        None,
        "test_invoice".to_string(),
        CurrencyUnit::Sat,
        Some(Amount::from(100)),
        unix_time() + 3600,
        "test_lookup_id".to_string(),
        None,
        Amount::ZERO,
        Amount::ZERO,
        PaymentMethod::Bolt11,
        unix_time(),
        vec![],
        vec![],
    );

    db.add_mint_quote(quote.clone()).await.unwrap();

    // Test: Try to insert the same payment_id concurrently from 10 tasks
    let payment_id = "duplicate_payment_id_test";
    let db = Arc::new(db);
    let mut set = JoinSet::new();

    for i in 0..10 {
        let db_clone = db.clone();
        let quote_id = quote.id.clone();
        let payment_id = payment_id.to_string();

        set.spawn(async move {
            let mut tx = db_clone.begin_transaction().await.unwrap();
            let result = tx.increment_mint_quote_amount_paid(
                &quote_id,
                Amount::from(10),
                payment_id,
            ).await;

            if result.is_ok() {
                tx.commit().await.unwrap();
            }

            (i, result)
        });
    }

    // Collect results
    let mut success_count = 0;
    let mut duplicate_errors = 0;
    let mut other_errors = 0;

    while let Some(result) = set.join_next().await {
        match result.unwrap().1 {
            Ok(_) => success_count += 1,
            Err(Error::Duplicate) => duplicate_errors += 1,
            Err(_) => other_errors += 1,
        }
    }

    // Verify: Exactly one insertion should succeed, rest should get Duplicate error
    assert_eq!(success_count, 1, "Exactly one insertion should succeed");
    assert_eq!(duplicate_errors, 9, "Nine should get duplicate error");
    assert_eq!(other_errors, 0, "No other errors should occur");

    // Verify: The quote should have been incremented exactly once
    let final_quote = db.get_mint_quote(&quote.id).await.unwrap().unwrap();
    assert_eq!(final_quote.amount_paid(), Amount::from(10));
    assert_eq!(final_quote.payments.len(), 1);
}

#[tokio::test]
async fn test_rapid_sequential_minting() {
    // This simulates the webapp "Mint 100 Tokens" scenario
    let db = setup_test_database().await;

    // Create 100 mint quotes
    for i in 0..100 {
        let quote = MintQuote::new(
            None,
            format!("invoice_{}", i),
            CurrencyUnit::Sat,
            Some(Amount::from(1)),
            unix_time() + 3600,
            format!("lookup_id_{}", i),
            None,
            Amount::ZERO,
            Amount::ZERO,
            PaymentMethod::Bolt11,
            unix_time(),
            vec![],
            vec![],
        );

        db.add_mint_quote(quote.clone()).await.unwrap();

        // Process payment with unique payment_id
        let mut tx = db.begin_transaction().await.unwrap();
        let result = tx.increment_mint_quote_amount_paid(
            &quote.id,
            Amount::from(1),
            format!("payment_{}", i),
        ).await;

        assert!(result.is_ok(), "Payment {} should succeed", i);
        tx.commit().await.unwrap();
    }

    println!("Successfully processed 100 sequential payments without race condition!");
}

async fn setup_test_database() -> impl Database {
    // Setup code for test database
    // This would use the actual CDK test utilities
    todo!("Implement using CDK test setup")
}
