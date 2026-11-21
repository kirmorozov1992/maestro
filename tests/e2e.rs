#![cfg(unix)]

#[path = "support/e2e.rs"]
mod support;

use std::time::Duration;

use serde_json::json;
use support::Harness;

#[tokio::test]
async fn starts_isolated_stands_and_registers_agents() {
    let (first, second) = tokio::join!(
        Harness::start(Duration::from_secs(20)),
        Harness::start(Duration::from_secs(20))
    );
    let mut first = first.unwrap();
    let mut second = second.unwrap();
    let first_root = first.working_dir().to_owned();
    let second_root = second.working_dir().to_owned();

    assert_ne!(first.address(), second.address());
    assert_ne!(first_root, second_root);
    assert!(first_root.is_dir() && second_root.is_dir());

    let (first_agent, second_agent) = tokio::join!(first.start_agent(), second.start_agent());
    assert_ne!(first_agent.unwrap(), second_agent.unwrap());
    assert_eq!(
        first.cli_json(&["status", "--json"]).await.unwrap(),
        json!({"items": []})
    );

    drop(second);
    drop(first);
    assert!(!first_root.exists() && !second_root.exists());
}
