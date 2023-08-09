use tokio_stream::StreamExt;
use xline_discovery::{
    host::Host,
    service::{ServiceInfo, ServiceRegistration},
    ClientError, EventType,
};
use xline_test_utils::Cluster;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn service_get_will_succeed_after_register() -> Result<()> {
    let (_cluster, registration) = get_cluster_registration().await?;

    let service_name = "test-service";

    registration
        .register_service(ServiceInfo::new(service_name))
        .await?;

    let service = registration.get_service(service_name).await?;
    assert_eq!(service.info().name, service_name);

    Ok(())
}

#[tokio::test]
async fn host_register_deregister_will_succeed() -> Result<()> {
    let (_cluster, registration) = get_cluster_registration().await?;

    let service_name = "test-service";

    let service = registration
        .register_service(ServiceInfo::new(service_name))
        .await?;

    let host = Host::new("host0", "host0.example.com", service_name, [("https", 443)]);
    service.register_host(host.clone()).await?;

    let (_revision, host_got) = service.one_host().await?;
    assert_eq!(host, host_got);

    service.deregister_host("host0").await?;
    assert!(service.one_host().await.is_err());

    Ok(())
}

#[tokio::test]
async fn watch_host_will_get_correct_info() -> Result<()> {
    let (_cluster, registration) = get_cluster_registration().await?;

    let service_name = "test-service";

    let service = registration
        .register_service(ServiceInfo::new(service_name))
        .await?;
    let service_c = service.clone();

    let host = Host::new("host0", "host0.example.com", service_name, [("https", 443)]);
    let host_c = host.clone();
    let mut host_stream = service.subscribe_all(0).await?;

    let handle = tokio::spawn(async move {
        service_c.register_host(host_c.clone()).await?;

        let (_revision, host_got) = service_c.one_host().await?;
        assert_eq!(host_c, host_got);

        service_c.deregister_host("host0").await?;
        assert!(service_c.one_host().await.is_err());

        Ok::<(), ClientError>(())
    });

    let (event_type, host_received) = host_stream.next().await.expect("no host received");
    assert_eq!(host_received, host);
    assert_eq!(event_type, EventType::Put);
    let (event_type, host_received) = host_stream.next().await.expect("no host received");
    assert_eq!(host_received, host);
    assert_eq!(event_type, EventType::Delete);

    handle.await.unwrap().unwrap();

    Ok(())
}

async fn get_cluster_registration() -> Result<(Cluster, ServiceRegistration)> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let members = cluster.addrs();
    let registration = ServiceRegistration::connect(members).await?;

    Ok((cluster, registration))
}
