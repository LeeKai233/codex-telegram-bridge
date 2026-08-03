#[path = "../src/control.rs"]
mod control;

use control::{ControlController, ControlEffect, ControlRequest};
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Deserialize)]
struct Fixture {
    name: String,
    request: ControlRequest,
    effects: Vec<ControlEffect>,
}

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../fixtures/control_contract/9527.json")
}

#[test]
fn normalized_9527_effect_fixtures_match_the_control_contract() {
    let fixtures: Vec<Fixture> =
        serde_json::from_str(&std::fs::read_to_string(fixture_path()).unwrap()).unwrap();
    let controller = ControlController;
    for fixture in fixtures {
        assert_eq!(
            controller.dispatch(fixture.request).unwrap(),
            fixture.effects,
            "fixture={}",
            fixture.name
        );
    }
}
