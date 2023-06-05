use xline_client::{
    clients::{kv::PutRequest, watch::WatchRequest},
    error::Result,
    Client, ClientOptions,
};
use xline_test_utils::Cluster;
use xlineapi::EventType;

#[tokio::test]
async fn watch() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await?;
    let mut watch_client = client.watch_client();
    let mut kv_client = client.kv_client();

    let (mut watcher, mut stream) = watch_client.watch(WatchRequest::new("watch01")).await?;

    kv_client.put(PutRequest::new("watch01", "01")).await?;

    let resp = stream.message().await?.unwrap();
    assert_eq!(resp.watch_id, watcher.watch_id());
    assert_eq!(resp.events.len(), 1);

    let kv = resp.events[0].kv.as_ref().unwrap();
    assert_eq!(kv.key, b"watch01");
    assert_eq!(kv.value, b"01");
    assert_eq!(resp.events[0].r#type(), EventType::Put);

    watcher.cancel()?;

    let resp = stream.message().await?.unwrap();
    assert_eq!(resp.watch_id, watcher.watch_id());
    assert!(resp.canceled);

    Ok(())
}

async fn get_cluster_client() -> Result<(Cluster, Client)> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = Client::connect(cluster.addrs().clone(), ClientOptions::default()).await?;
    Ok((cluster, client))
}
