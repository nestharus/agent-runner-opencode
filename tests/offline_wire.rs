use agent_runner_opencode::dispatch::decode_request;
use serde_json::{json, Value};

#[test]
fn pinned_requests_reach_the_provider_decoder() {
    let fixtures: Value = serde_json::from_str(include_str!("fixtures/sdk-contract.json")).unwrap();
    let operations = fixtures["non_launch"].as_object().unwrap();
    for (operation, fixture) in operations {
        let request = &fixture["request"];
        let actual = decode_request(&serde_json::to_vec(request).unwrap()).unwrap();
        println!("{} {} {}", operation, actual.request_id, actual.params);
        assert_eq!(actual.request_id, request["request_id"]);
        assert_eq!(actual.params, request["params"]);
        assert_eq!(actual.host.app, request["host"]["app"]);
        let mut invalid = request.clone();
        invalid["contract"] = json!("oulipoly.provider/v0");
        assert!(decode_request(&serde_json::to_vec(&invalid).unwrap()).is_err());
    }
    let request = &fixtures["launch"]["request"];
    let decoded = decode_request(&serde_json::to_vec(request).unwrap()).unwrap();
    assert_eq!(decoded.params, request["params"]);
    assert_eq!(operations.len(), 30);
}

#[test]
fn deadline_accepts_integer_decimal_representation() {
    let fixtures: Value = serde_json::from_str(include_str!("fixtures/sdk-contract.json")).unwrap();
    let mut request = fixtures["non_launch"]["describe"]["request"].clone();
    request["host"]["deadline_unix_ms"] = serde_json::from_str("80.0").unwrap();
    let result = decode_request(&serde_json::to_vec(&request).unwrap());
    println!("{result:?}");
    assert!(result.is_ok());
}
