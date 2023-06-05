use xline_client::{
    clients::lock::{LockClient, LockRequest, UnlockRequest},
    error::Result,
    Client, ClientOptions,
};
use xline_test_utils::Cluster;

// TODO: enable the tests when the curp server is properly implemented lease grant
#[tokio::test]
async fn lock() -> Result<()> {
    let (_cluster, mut client) = get_cluster_client().await?;
    let resp = client
        .lock(LockRequest::new().with_name("lock-test"))
        .await?;
    let key = resp.key;
    let key_str = std::str::from_utf8(&key).unwrap();
    assert!(key_str.starts_with("lock-test/"));

    client.unlock(UnlockRequest::new().with_key(key)).await?;
    Ok(())
}

async fn get_cluster_client() -> Result<(Cluster, LockClient)> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = Client::connect(cluster.addrs().clone(), ClientOptions::default())
        .await?
        .lock_client();
    Ok((cluster, client))
}
