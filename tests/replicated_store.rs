//use assert_cmd::prelude::*;
//use std::{process::Command, thread, time::Duration};
//use tempfile::TempDir;
//
//#[test]
//fn replicated_setup() {
//    let t1 = TempDir::new().unwrap();
//    let t2 = TempDir::new().unwrap();
//    let t3 = TempDir::new().unwrap();
//
//    let mut follower_one = Command::cargo_bin("sqrl-server").unwrap();
//    follower_one
//        .args(["--addr", "127.0.0.1:7001"])
//        .current_dir(&t1)
//        .spawn()
//        .unwrap();
//
//    let mut follower_two = Command::cargo_bin("sqrl-server").unwrap();
//    follower_two
//        .args(["--addr", "127.0.0.1:7002"])
//        .current_dir(&t2)
//        .spawn()
//        .unwrap();
//    let mut replication_server = Command::cargo_bin("sqrl-server").unwrap();
//    replication_server
//        .args([
//            "--replication-mode",
//            "leader",
//            "--addr",
//            "127.0.0.1:7000",
//            "--followers",
//            "127.0.0.1:7001,127.0.0.1:7002",
//        ])
//        .current_dir(&t3)
//        .spawn()
//        .unwrap();
//
//    thread::sleep(Duration::from_secs(1));
//    Command::cargo_bin("sqrl-client")
//        .unwrap()
//        .args(["set", "key1", "value1", "--addr", "127.0.0.1:7000"])
//        .spawn()
//        .unwrap();
//
//    thread::sleep(Duration::from_secs(1));
//    Command::cargo_bin("sqrl-client")
//        .unwrap()
//        .args(["get", "key1", "--addr", "127.0.0.1:7000"])
//        .assert()
//        .stdout("value1");
//}
