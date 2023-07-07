use xline_client::{
    error::ClientError as Error,
    types::{kv::PutRequest, watch::WatchRequest},
    Client, ClientOptions,
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    // the name and address of all curp members
    let curp_members = [
        ("server0", "10.0.0.1:2379"),
        ("server1", "10.0.0.2:2379"),
        ("server2", "10.0.0.3:2379"),
    ]
    .map(|(s, a)| (s.to_owned(), a.to_owned()));

    let client = Client::connect(curp_members.into(), ClientOptions::default()).await?;
    let mut watch_client = client.watch_client();
    let mut kv_client = client.kv_client();

    // watch
    let (mut watcher, mut stream) = watch_client.watch(WatchRequest::new("key1")).await?;
    kv_client.put(PutRequest::new("key1", "value1")).await?;

    let resp = stream.message().await?.unwrap();
    let kv = resp.events[0].kv.as_ref().unwrap();

    println!(
        "got key: {}, value: {}",
        String::from_utf8_lossy(&kv.key),
        String::from_utf8_lossy(&kv.value)
    );

    // cancel the watch
    watcher.cancel()?;

    Ok(())
}
