use common::get_cluster_client;
use xline_client::error::Result;

mod common;

#[tokio::test]
async fn snapshot() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await?;
    let mut client = client.maintenance_client();

    let mut msg = client.snapshot().await?;
    loop {
        if let Some(resp) = msg.message().await? {
            assert!(!resp.blob.is_empty());
            if resp.remaining_bytes == 0 {
                break;
            }
        }
    }
    Ok(())
}
