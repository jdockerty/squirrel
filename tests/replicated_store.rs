//use std::{thread, time::Duration};
//
//use sqrl::client::{self, Client, RemoteNodeClient, ReplicationClient};
//use tempfile::TempDir;
//
//async fn client_one() -> RemoteNodeClient {
//RemoteNodeClient::new("127.0.0.1:6000".to_string()).await.unwrap()
//}
//
//async fn client_two() -> RemoteNodeClient {
//RemoteNodeClient::new("127.0.0.1:6001".to_string()).await.unwrap()
//}
//
//#[tokio::test]
//async fn general_replication() {
//
//    let node_one = ReplicationClient::new(
//        vec![RemoteNodeClient::new("127.0.0.1:6001".to_string()).await.unwrap()],
//        TempDir::new().unwrap().into_path(),
//        "127.0.0.1:6000".parse().unwrap(),
//    );
//
//    let node_one = ReplicationClient::new(
//        vec![RemoteNodeClient::new("127.0.0.1:6000".to_string()).await.unwrap()],
//        TempDir::new().unwrap().into_path(),
//        "127.0.0.1:6001".parse().unwrap(),
//    );
//
//    thread::sleep(Duration::from_secs(1));
//
//    let mut client = client_one().await;
//    client.set("key1".to_string(), "value1".to_string()).await.unwrap();
//
//    let mut client = client_two().await;
//    assert_eq!(client.get("key1".to_string()).await.unwrap().unwrap().value, Some("value1".to_string()));
//}
