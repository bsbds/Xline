//! The binary is to quickly setup a cluster for convenient testing
use tokio::signal;
use xline_test_utils::Cluster;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_target(true)
        .init();
    let mut cluster =
        Cluster::new_with_addrs(vec!["127.0.0.1:2379", "127.0.0.1:2380", "127.0.0.1:2381"]).await;
    cluster.start().await;

    println!("cluster running");

    for (id, addr) in cluster.all_members_client_urls_map() {
        println!("server id: {} addr: {}", id, addr);
    }

    if let Err(e) = signal::ctrl_c().await {
        eprintln!("Unable to listen for shutdown signal: {e}");
    }
}
