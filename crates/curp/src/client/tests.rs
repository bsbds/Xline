use std::{collections::HashMap, sync::Arc, time::Duration};

use curp_test_utils::test_cmd::TestCommand;
#[cfg(not(madsim))]
use tonic::transport::ClientTlsConfig;
use tracing_test::traced_test;
#[cfg(madsim)]
use utils::ClientTlsConfig;

use super::{
    state::State,
    unary::{Unary, UnaryConfig},
};
use crate::{
    client::ClientApi,
    members::ServerId,
    rpc::{
        connect::{ConnectApi, MockConnectApi},
        CurpError, FetchClusterResponse, Member,
    },
};

/// Create a mocked connects with server id from 0~size
#[allow(trivial_casts)] // Trait object with high ranked type inferences failed, cast manually
fn init_mocked_connects(
    size: usize,
    f: impl Fn(usize, &mut MockConnectApi),
) -> HashMap<ServerId, Arc<dyn ConnectApi>> {
    std::iter::repeat_with(|| MockConnectApi::new())
        .take(size)
        .enumerate()
        .map(|(id, mut conn)| {
            conn.expect_id().returning(move || id as ServerId);
            conn.expect_update_addrs().returning(|_addr| Ok(()));
            f(id, &mut conn);
            (id as ServerId, Arc::new(conn) as Arc<dyn ConnectApi>)
        })
        .collect()
}

/// Create unary client for test
fn init_unary_client(
    connects: HashMap<ServerId, Arc<dyn ConnectApi>>,
    local_server: Option<ServerId>,
    leader: Option<ServerId>,
    term: u64,
    cluster_version: u64,
    tls_config: Option<ClientTlsConfig>,
) -> Unary<TestCommand> {
    let state = State::new_arc(
        connects,
        local_server,
        leader,
        term,
        cluster_version,
        tls_config,
    );
    Unary::new(
        state,
        UnaryConfig::new(Duration::from_secs(0), Duration::from_secs(0)),
    )
}

// Tests for unary client

#[traced_test]
#[tokio::test]
async fn test_unary_fetch_clusters_serializable() {
    let connects = init_mocked_connects(3, |_id, conn| {
        conn.expect_fetch_cluster().return_once(|_req, _timeout| {
            Ok(tonic::Response::new(FetchClusterResponse {
                leader_id: Some(0),
                term: 1,
                cluster_id: 123,
                members: vec![
                    Member::new(0, "S0", vec!["A0".to_owned()], [], false),
                    Member::new(1, "S1", vec!["A1".to_owned()], [], false),
                    Member::new(2, "S2", vec!["A2".to_owned()], [], false),
                ],
                cluster_version: 1,
            }))
        });
    });
    let unary = init_unary_client(connects, None, None, 0, 0, None);
    let res = unary.fetch_cluster(false).await.unwrap();
    assert_eq!(
        res.into_peer_urls(),
        HashMap::from([
            (0, vec!["A0".to_owned()]),
            (1, vec!["A1".to_owned()]),
            (2, vec!["A2".to_owned()])
        ])
    );
}

#[traced_test]
#[tokio::test]
async fn test_unary_fetch_clusters_serializable_local_first() {
    let connects = init_mocked_connects(3, |id, conn| {
        conn.expect_fetch_cluster()
            .return_once(move |_req, _timeout| {
                let members = if id == 1 {
                    // local server(1) does not see the cluster members
                    vec![]
                } else {
                    panic!("other server's `fetch_cluster` should not be invoked");
                };
                Ok(tonic::Response::new(FetchClusterResponse {
                    leader_id: Some(0),
                    term: 1,
                    cluster_id: 123,
                    members,
                    cluster_version: 1,
                }))
            });
    });
    let unary = init_unary_client(connects, Some(1), None, 0, 0, None);
    let res = unary.fetch_cluster(false).await.unwrap();
    assert!(res.members.is_empty());
}

#[traced_test]
#[tokio::test]
async fn test_unary_fetch_clusters_linearizable() {
    let connects = init_mocked_connects(5, |id, conn| {
        conn.expect_fetch_cluster()
            .return_once(move |_req, _timeout| {
                let resp = match id {
                    0 => FetchClusterResponse {
                        leader_id: Some(0),
                        term: 2,
                        cluster_id: 123,
                        members: vec![
                            Member::new(0, "S0", vec!["A0".to_owned()], [], false),
                            Member::new(1, "S1", vec!["A1".to_owned()], [], false),
                            Member::new(2, "S2", vec!["A2".to_owned()], [], false),
                            Member::new(3, "S3", vec!["A3".to_owned()], [], false),
                            Member::new(4, "S4", vec!["A4".to_owned()], [], false),
                        ],
                        cluster_version: 1,
                    },
                    1 | 4 => FetchClusterResponse {
                        leader_id: Some(0),
                        term: 2,
                        cluster_id: 123,
                        members: vec![], // linearizable read from follower returns empty members
                        cluster_version: 1,
                    },
                    2 => FetchClusterResponse {
                        leader_id: None,
                        term: 23, // abnormal term
                        cluster_id: 123,
                        members: vec![],
                        cluster_version: 1,
                    },
                    3 => FetchClusterResponse {
                        leader_id: Some(3), // imagine this node is a old leader
                        term: 1,            // with the old term
                        cluster_id: 123,
                        members: vec![
                            Member::new(0, "S0", vec!["B0".to_owned()], [], false),
                            Member::new(1, "S1", vec!["B1".to_owned()], [], false),
                            Member::new(2, "S2", vec!["B2".to_owned()], [], false),
                            Member::new(3, "S3", vec!["B3".to_owned()], [], false),
                            Member::new(4, "S4", vec!["B4".to_owned()], [], false),
                        ],
                        cluster_version: 1,
                    },
                    _ => unreachable!("there are only 5 nodes"),
                };
                Ok(tonic::Response::new(resp))
            });
    });
    let unary = init_unary_client(connects, None, None, 0, 0, None);
    let res = unary.fetch_cluster(true).await.unwrap();
    assert_eq!(
        res.into_peer_urls(),
        HashMap::from([
            (0, vec!["A0".to_owned()]),
            (1, vec!["A1".to_owned()]),
            (2, vec!["A2".to_owned()]),
            (3, vec!["A3".to_owned()]),
            (4, vec!["A4".to_owned()])
        ])
    );
}

#[traced_test]
#[tokio::test]
async fn test_unary_fetch_clusters_linearizable_failed() {
    let connects = init_mocked_connects(5, |id, conn| {
        conn.expect_fetch_cluster()
            .return_once(move |_req, _timeout| {
                let resp = match id {
                    0 => FetchClusterResponse {
                        leader_id: Some(0),
                        term: 2,
                        cluster_id: 123,
                        members: vec![
                            Member::new(0, "S0", vec!["A0".to_owned()], [], false),
                            Member::new(1, "S1", vec!["A1".to_owned()], [], false),
                            Member::new(2, "S2", vec!["A2".to_owned()], [], false),
                            Member::new(3, "S3", vec!["A3".to_owned()], [], false),
                            Member::new(4, "S4", vec!["A4".to_owned()], [], false),
                        ],
                        cluster_version: 1,
                    },
                    1 => FetchClusterResponse {
                        leader_id: Some(0),
                        term: 2,
                        cluster_id: 123,
                        members: vec![], // linearizable read from follower returns empty members
                        cluster_version: 1,
                    },
                    2 => FetchClusterResponse {
                        leader_id: None, // imagine this node is a disconnected candidate
                        term: 23,        // with a high term
                        cluster_id: 123,
                        members: vec![],
                        cluster_version: 1,
                    },
                    3 => FetchClusterResponse {
                        leader_id: Some(3), // imagine this node is a old leader
                        term: 1,            // with the old term
                        cluster_id: 123,
                        members: vec![
                            Member::new(0, "S0", vec!["B0".to_owned()], [], false),
                            Member::new(1, "S1", vec!["B1".to_owned()], [], false),
                            Member::new(2, "S2", vec!["B2".to_owned()], [], false),
                            Member::new(3, "S3", vec!["B3".to_owned()], [], false),
                            Member::new(4, "S4", vec!["B4".to_owned()], [], false),
                        ],
                        cluster_version: 1,
                    },
                    4 => FetchClusterResponse {
                        leader_id: Some(3), // imagine this node is a old follower of old leader(3)
                        term: 1,            // with the old term
                        cluster_id: 123,
                        members: vec![],
                        cluster_version: 1,
                    },
                    _ => unreachable!("there are only 5 nodes"),
                };
                Ok(tonic::Response::new(resp))
            });
    });
    let unary = init_unary_client(connects, None, None, 0, 0, None);
    let res = unary.fetch_cluster(true).await.unwrap_err();
    // only server(0, 1)'s responses are valid, less than majority quorum(3), got a mocked RpcTransport to retry
    assert_eq!(res, CurpError::RpcTransport(()));
}

// TODO: rewrite this tests
#[cfg(ignore)]
#[traced_test]
#[tokio::test]
async fn test_unary_propose_fast_path_works() {
    let connects = init_mocked_connects(5, |id, conn| {
        conn.expect_propose()
            .return_once(move |_req, _token, _timeout| {
                let resp = match id {
                    0 => ProposeResponse::new_result::<TestCommand>(
                        &Ok(TestCommandResult::default()),
                        false,
                    ),
                    1 | 2 | 3 => ProposeResponse::new_empty(),
                    4 => return Err(CurpError::key_conflict()),
                    _ => unreachable!("there are only 5 nodes"),
                };
                Ok(tonic::Response::new(resp))
            });
        conn.expect_wait_synced()
            .return_once(move |_req, _timeout| {
                assert!(id == 0, "wait synced should send to leader");
                std::thread::sleep(Duration::from_millis(100));
                Ok(tonic::Response::new(WaitSyncedResponse::new_from_result::<
                    TestCommand,
                >(
                    Ok(TestCommandResult::default()),
                    Some(Ok(1.into())),
                )))
            });
    });
    let unary = init_unary_client(connects, None, Some(0), 1, 0, None);
    let res = unary
        .propose(&TestCommand::default(), None, true)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(res, (TestCommandResult::default(), None));
}

// TODO: rewrite this tests
#[cfg(ignore)]
#[traced_test]
#[tokio::test]
async fn test_unary_propose_slow_path_works() {
    let connects = init_mocked_connects(5, |id, conn| {
        conn.expect_propose()
            .return_once(move |_req, _token, _timeout| {
                let resp = match id {
                    0 => ProposeResponse::new_result::<TestCommand>(
                        &Ok(TestCommandResult::default()),
                        false,
                    ),
                    1 | 2 | 3 => ProposeResponse::new_empty(),
                    4 => return Err(CurpError::key_conflict()),
                    _ => unreachable!("there are only 5 nodes"),
                };
                Ok(tonic::Response::new(resp))
            });
        conn.expect_wait_synced()
            .return_once(move |_req, _timeout| {
                assert!(id == 0, "wait synced should send to leader");
                std::thread::sleep(Duration::from_millis(100));
                Ok(tonic::Response::new(WaitSyncedResponse::new_from_result::<
                    TestCommand,
                >(
                    Ok(TestCommandResult::default()),
                    Some(Ok(1.into())),
                )))
            });
    });
    let unary = init_unary_client(connects, None, Some(0), 1, 0, None);
    let start_at = Instant::now();
    let res = unary
        .propose(&TestCommand::default(), None, false)
        .await
        .unwrap()
        .unwrap();
    assert!(
        start_at.elapsed() > Duration::from_millis(100),
        "slow round takes at least 100ms"
    );
    assert_eq!(
        res,
        (TestCommandResult::default(), Some(LogIndexResult::from(1)))
    );
}

// TODO: rewrite this tests
#[cfg(ignore)]
#[traced_test]
#[tokio::test]
async fn test_unary_propose_fast_path_fallback_slow_path() {
    // record how many times `handle_propose` was invoked.
    let counter = Arc::new(Mutex::new(0));
    let connects = init_mocked_connects(5, |id, conn| {
        let counter_c = Arc::clone(&counter);
        conn.expect_propose()
            .return_once(move |_req, _token, _timeout| {
                counter_c.lock().unwrap().add_assign(1);
                // insufficient quorum
                let resp = match id {
                    0 => ProposeResponse::new_result::<TestCommand>(
                        &Ok(TestCommandResult::default()),
                        false,
                    ),
                    1 | 2 => ProposeResponse::new_empty(),
                    3 | 4 => return Err(CurpError::key_conflict()),
                    _ => unreachable!("there are only 5 nodes"),
                };
                Ok(tonic::Response::new(resp))
            });
        conn.expect_wait_synced()
            .return_once(move |_req, _timeout| {
                assert!(id == 0, "wait synced should send to leader");
                std::thread::sleep(Duration::from_millis(100));
                Ok(tonic::Response::new(WaitSyncedResponse::new_from_result::<
                    TestCommand,
                >(
                    Ok(TestCommandResult::default()),
                    Some(Ok(1.into())),
                )))
            });
    });
    let unary = init_unary_client(connects, None, Some(0), 1, 0, None);
    let start_at = Instant::now();
    let res = unary
        .propose(&TestCommand::default(), None, true)
        .await
        .unwrap()
        .unwrap();
    assert!(
        start_at.elapsed() > Duration::from_millis(100),
        "slow round takes at least 100ms"
    );
    // indicate that we actually run out of fast round
    assert_eq!(*counter.lock().unwrap(), 5);
    assert_eq!(
        res,
        (TestCommandResult::default(), Some(LogIndexResult::from(1)))
    );
}

// TODO: rewrite this tests
#[cfg(ignore)]
#[traced_test]
#[tokio::test]
async fn test_unary_propose_return_early_err() {
    for early_err in [
        CurpError::shutting_down(),
        CurpError::invalid_config(),
        CurpError::node_already_exists(),
        CurpError::node_not_exist(),
        CurpError::learner_not_catch_up(),
        CurpError::expired_client_id(),
        CurpError::redirect(Some(1), 0),
    ] {
        assert!(early_err.should_abort_fast_round());
        // record how many times rpc was invoked.
        let counter = Arc::new(Mutex::new(0));
        let connects = init_mocked_connects(5, |id, conn| {
            let err = early_err.clone();
            let counter_c = Arc::clone(&counter);
            conn.expect_propose()
                .return_once(move |_req, _token, _timeout| {
                    counter_c.lock().unwrap().add_assign(1);
                    Err(err)
                });
            let err = early_err.clone();
            let counter_c = Arc::clone(&counter);
            conn.expect_wait_synced()
                .return_once(move |_req, _timeout| {
                    assert!(id == 0, "wait synced should send to leader");
                    counter_c.lock().unwrap().add_assign(1);
                    Err(err)
                });
        });
        let unary = init_unary_client(connects, None, Some(0), 1, 0, None);
        let err = unary
            .propose(&TestCommand::default(), None, true)
            .await
            .unwrap_err();
        assert_eq!(err, early_err);
        assert_eq!(*counter.lock().unwrap(), 1);
    }
}

// Tests for retry layer

// TODO: rewrite this tests
#[cfg(ignore)]
#[traced_test]
#[tokio::test]
async fn test_retry_propose_return_no_retry_error() {
    for early_err in [
        CurpError::shutting_down(),
        CurpError::invalid_config(),
        CurpError::node_already_exists(),
        CurpError::node_not_exist(),
        CurpError::learner_not_catch_up(),
    ] {
        // record how many times rpc was invoked.
        let counter = Arc::new(Mutex::new(0));
        let connects = init_mocked_connects(5, |id, conn| {
            let err = early_err.clone();
            let counter_c = Arc::clone(&counter);
            conn.expect_propose()
                .return_once(move |_req, _token, _timeout| {
                    counter_c.lock().unwrap().add_assign(1);
                    Err(err)
                });
            let err = early_err.clone();
            let counter_c = Arc::clone(&counter);
            conn.expect_wait_synced()
                .return_once(move |_req, _timeout| {
                    assert!(id == 0, "wait synced should send to leader");
                    counter_c.lock().unwrap().add_assign(1);
                    Err(err)
                });
        });
        let unary = init_unary_client(connects, None, Some(0), 1, 0, None);
        let retry = Retry::new(unary, RetryConfig::new_fixed(Duration::from_millis(100), 5));
        let err = retry
            .propose(&TestCommand::default(), None, false)
            .await
            .unwrap_err();
        assert_eq!(err.message(), tonic::Status::from(early_err).message());
        // fast path + slow path = 2
        assert_eq!(*counter.lock().unwrap(), 2);
    }
}

// TODO: rewrite this tests
#[cfg(ignore)]
#[traced_test]
#[tokio::test]
async fn test_retry_propose_return_retry_error() {
    for early_err in [
        CurpError::expired_client_id(),
        CurpError::key_conflict(),
        CurpError::RpcTransport(()),
        CurpError::internal("No reason"),
    ] {
        let connects = init_mocked_connects(5, |id, conn| {
            let err = early_err.clone();
            conn.expect_fetch_cluster()
                .returning(move |_req, _timeout| {
                    Ok(tonic::Response::new(FetchClusterResponse {
                        leader_id: Some(0),
                        term: 2,
                        cluster_id: 123,
                        members: vec![
                            Member::new(0, "S0", vec!["A0".to_owned()], [], false),
                            Member::new(1, "S1", vec!["A1".to_owned()], [], false),
                            Member::new(2, "S2", vec!["A2".to_owned()], [], false),
                            Member::new(3, "S3", vec!["A3".to_owned()], [], false),
                            Member::new(4, "S4", vec!["A4".to_owned()], [], false),
                        ],
                        cluster_version: 1,
                    }))
                });
            conn.expect_propose()
                .returning(move |_req, _token, _timeout| Err(err.clone()));
            if id == 0 {
                let err = early_err.clone();
                conn.expect_wait_synced()
                    .times(5) // wait synced should be retried in 5 times on leader
                    .returning(move |_req, _timeout| Err(err.clone()));
            }
        });
        let unary = init_unary_client(connects, None, Some(0), 1, 0, None);
        let retry = Retry::new(unary, RetryConfig::new_fixed(Duration::from_millis(10), 5));
        let err = retry
            .propose(&TestCommand::default(), None, false)
            .await
            .unwrap_err();
        assert!(err.message().contains("request timeout"));
    }
}
