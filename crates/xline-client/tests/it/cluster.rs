use test_macros::abort_on_panic;
use xline_client::{
    clients::XlineMembership,
    error::Result,
    types::cluster::{Change, Node},
};

use super::common::get_cluster_client;

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn learner_add_and_remove_are_ok() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let client = client.cluster_client();

    let node1 = Node::new(11, "n1", vec!["10.0.0.4:2380"], vec!["10.0.0.4.2379"]);
    let node2 = Node::new(12, "n2", vec!["10.0.0.5:2380"], vec!["10.0.0.5.2379"]);
    client
        .change(vec![Change::Add(node1), Change::Add(node2)])
        .await
        .expect("failed to add learners");

    // Remove the previously added learners
    client
        .change(vec![Change::Remove(11), Change::Remove(12)])
        .await
        .expect("failed to remove learners");

    Ok(())
}
