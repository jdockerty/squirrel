use assert_cmd::prelude::*;
use std::process::Command;
use tempfile::TempDir;

#[test]
fn replicated_setup() {
    let temp_dir = TempDir::new().unwrap();
    let mut cmd = Command::cargo_bin("sqrl-server").unwrap();
    cmd.args(["--replication-mode",  "leader", "--addr", "127.0.0.1:7000"])
        .current_dir(&temp_dir).assert().success();
}
