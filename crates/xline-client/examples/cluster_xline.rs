use anyhow::Result;
use futures::StreamExt;
use xline_client::{
    clients::XlineMembership,
    types::cluster::{Change, LearnerStatus, Node},
    Client, ClientOptions,
};

#[tokio::main]
async fn main() -> Result<()> {
    // the name and address of all curp members
    let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];

    let client = Client::connect(curp_members, ClientOptions::default())
        .await?
        .cluster_client();

    let node1 = Node::new(1, "n1", vec!["10.0.0.4:2380"], vec!["10.0.0.4.2379"]);
    let node2 = Node::new(2, "n2", vec!["10.0.0.5:2380"], vec!["10.0.0.5.2379"]);
    client
        .change(vec![Change::Add(node1), Change::Add(node2)])
        .await?;

    // waits learner to be ready
    let mut stream = client.wait_learner([1, 2]).await?;
    while let Some(Ok(notify)) = stream.next().await {
        match notify {
            LearnerStatus::Pending { node_id, index } => {
                println!("learner {node_id} match index updates to {index}");
            }
            LearnerStatus::Ready => {
                println!("learner is ready");
            }
        }
    }

    client.change(vec![Change::Promote(1)]).await?;
    client.change(vec![Change::Demote(1)]).await?;
    // Remove the previously added learners
    client
        .change(vec![Change::Remove(1), Change::Remove(2)])
        .await?;

    Ok(())
}
