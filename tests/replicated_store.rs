//#[tokio::test]
//async fn general_replication() {
//    let nodes = vec![SqrlNode{addr: "127.0.0.1:5000"}, SqrlNode{addr:"127.0.0.1:5001"}];
//    let client = sqrl::client::Client::new("127.0.0.1:5000");
//
//    client.set("foo", "foo").await.unwrap();
//    assert_eq!(client.get("foo").await.unwrap(), Some("foo"));
//
//    let client = sqrl::client::Client::new("127.0.0.1:5001");
//    assert_eq!(client.get("foo").await.unwrap(), Some("foo"));
//
//}
