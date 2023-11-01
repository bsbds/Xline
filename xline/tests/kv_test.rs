use std::{error::Error, time::Duration};

use etcd_client::{Client, Compare, CompareOp, GetOptions, KvClient, Txn, TxnOp, TxnOpResponse};
use futures::future::join_all;
use test_macros::abort_on_panic;
use xline::client::kv_types::{
    DeleteRangeRequest, PutRequest, RangeRequest, SortOrder, SortTarget,
};
use xline_test_utils::Cluster;

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_kv_put() -> Result<(), Box<dyn Error>> {
    struct TestCase {
        req: PutRequest,
        want_err: bool,
    }

    let tests = [
        TestCase {
            req: PutRequest::new("foo", "").with_ignore_value(true),
            want_err: true,
        },
        TestCase {
            req: PutRequest::new("foo", "bar"),
            want_err: false,
        },
        TestCase {
            req: PutRequest::new("foo", "").with_ignore_value(true),
            want_err: false,
        },
    ];

    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await;

    for test in tests {
        let res = client.put(test.req).await;
        assert_eq!(res.is_err(), test.want_err);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_kv_get() -> Result<(), Box<dyn Error>> {
    struct TestCase<'a> {
        req: RangeRequest,
        want_kvs: &'a [&'a str],
    }

    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await;

    let kvs = ["a", "b", "c", "c", "c", "foo", "foo/abc", "fop"];
    let want_kvs = ["a", "b", "c", "foo", "foo/abc", "fop"];
    let kvs_by_version = ["a", "b", "foo", "foo/abc", "fop", "c"];
    let reversed_kvs = ["fop", "foo/abc", "foo", "c", "b", "a"];

    let tests = [
        TestCase {
            req: RangeRequest::new("a"),
            want_kvs: &want_kvs[..1],
        },
        TestCase {
            req: RangeRequest::new("a").with_serializable(true),
            want_kvs: &want_kvs[..1],
        },
        TestCase {
            req: RangeRequest::new("a").with_range_end("c"),
            want_kvs: &want_kvs[..2],
        },
        TestCase {
            req: RangeRequest::new("").with_prefix(),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            req: RangeRequest::new("").with_from_key(),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            req: RangeRequest::new("a").with_range_end("x"),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            req: RangeRequest::new("").with_prefix().with_revision(4),
            want_kvs: &want_kvs[..3],
        },
        TestCase {
            req: RangeRequest::new("a").with_count_only(true),
            want_kvs: &[],
        },
        TestCase {
            req: RangeRequest::new("foo").with_prefix(),
            want_kvs: &["foo", "foo/abc"],
        },
        TestCase {
            req: RangeRequest::new("foo").with_from_key(),
            want_kvs: &["foo", "foo/abc", "fop"],
        },
        TestCase {
            req: RangeRequest::new("").with_prefix().with_limit(2),
            want_kvs: &want_kvs[..2],
        },
        TestCase {
            req: RangeRequest::new("")
                .with_prefix()
                .with_sort_target(SortTarget::Mod)
                .with_sort_order(SortOrder::Ascend),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            req: RangeRequest::new("")
                .with_prefix()
                .with_sort_target(SortTarget::Version)
                .with_sort_order(SortOrder::Ascend),
            want_kvs: &kvs_by_version[..],
        },
        TestCase {
            req: RangeRequest::new("")
                .with_prefix()
                .with_sort_target(SortTarget::Create)
                .with_sort_order(SortOrder::None),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            req: RangeRequest::new("")
                .with_prefix()
                .with_sort_target(SortTarget::Create)
                .with_sort_order(SortOrder::Descend),
            want_kvs: &reversed_kvs[..],
        },
        TestCase {
            req: RangeRequest::new("")
                .with_prefix()
                .with_sort_target(SortTarget::Key)
                .with_sort_order(SortOrder::Descend),
            want_kvs: &reversed_kvs[..],
        },
    ];

    for key in kvs {
        client.put(PutRequest::new(key, "bar")).await?;
    }

    for (i, test) in tests.into_iter().enumerate() {
        let res = client.range(test.req).await?;
        assert_eq!(res.kvs.len(), test.want_kvs.len());

        for (kv, want) in res.kvs.iter().zip(test.want_kvs.iter()) {
            assert!(
                kv.key == want.as_bytes(),
                "test: {i} failed, key: {:?}, want: {}",
                kv.key,
                want
            );
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_range_redirect() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;

    let addr = cluster.all_members()["server1"].clone();
    let mut kv_client = Client::connect([addr], None).await?.kv_client();
    let _ignore = kv_client.put("foo", "bar", None).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let res = kv_client.get("foo", None).await?;
    assert_eq!(res.kvs().len(), 1);
    assert_eq!(res.kvs()[0].value(), b"bar");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_kv_delete() -> Result<(), Box<dyn Error>> {
    struct TestCase<'a> {
        req: DeleteRangeRequest,
        want_deleted: i64,
        want_keys: &'a [&'a str],
    }

    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await;

    let keys = ["a", "b", "c", "c/abc", "d"];

    let tests = [
        TestCase {
            req: DeleteRangeRequest::new("").with_prefix(),
            want_deleted: 5,
            want_keys: &[],
        },
        TestCase {
            req: DeleteRangeRequest::new("").with_from_key(),
            want_deleted: 5,
            want_keys: &[],
        },
        TestCase {
            req: DeleteRangeRequest::new("a").with_range_end("c"),
            want_deleted: 2,
            want_keys: &["c", "c/abc", "d"],
        },
        TestCase {
            req: DeleteRangeRequest::new("c"),
            want_deleted: 1,
            want_keys: &["a", "b", "c/abc", "d"],
        },
        TestCase {
            req: DeleteRangeRequest::new("c").with_prefix(),
            want_deleted: 2,
            want_keys: &["a", "b", "d"],
        },
        TestCase {
            req: DeleteRangeRequest::new("c").with_from_key(),
            want_deleted: 3,
            want_keys: &["a", "b"],
        },
        TestCase {
            req: DeleteRangeRequest::new("e"),
            want_deleted: 0,
            want_keys: &keys,
        },
    ];

    for test in tests {
        for key in keys {
            client.put(PutRequest::new(key, "bar")).await?;
        }

        let res = client.delete(test.req).await?;
        assert_eq!(res.deleted, test.want_deleted);

        let res = client.range(RangeRequest::new("").with_prefix()).await?;
        let is_identical = res
            .kvs
            .iter()
            .zip(test.want_keys.iter())
            .all(|(kv, want)| kv.key == want.as_bytes());
        assert!(is_identical);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_txn() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let mut client: KvClient = cluster.client().await.kv_client();

    let kvs = ["a", "b", "c", "d", "e"];
    for key in kvs {
        client.put(key, "bar", None).await?;
    }

    let read_write_txn = Txn::new()
        .when([Compare::value("b", CompareOp::Equal, "bar")])
        .and_then([TxnOp::put("f", "foo", None)])
        .or_else([TxnOp::get("a", None)]);
    let res = client.txn(read_write_txn).await?;
    assert!(res.succeeded());
    assert_eq!(res.op_responses().len(), 1);
    assert!(matches!(res.op_responses()[0], TxnOpResponse::Put(_)));

    let read_only_txn = Txn::new()
        .when([Compare::version("b", CompareOp::Greater, 10)])
        .and_then([TxnOp::get("a", None)])
        .or_else([TxnOp::get("b", None)]);
    let res = client.txn(read_only_txn).await?;
    assert!(!res.succeeded());
    assert_eq!(res.op_responses().len(), 1);
    let Some(TxnOpResponse::Get(get_res)) = res.op_responses().pop() else {
        panic!("unexpected op response");
    };
    assert_eq!(get_res.kvs().len(), 1);
    assert_eq!(get_res.kvs()[0].key(), b"b");
    assert_eq!(get_res.kvs()[0].value(), b"bar");

    let serializable_txn = Txn::new()
        .when([])
        .and_then([TxnOp::get("c", Some(GetOptions::new().with_serializable()))])
        .or_else([TxnOp::get("d", Some(GetOptions::new().with_serializable()))]);
    let res = client.txn(serializable_txn).await?;
    assert!(res.succeeded());
    assert_eq!(res.op_responses().len(), 1);
    let Some(TxnOpResponse::Get(get_res)) = res.op_responses().pop() else {
        panic!("unexpected op response");
    };
    assert_eq!(get_res.kvs().len(), 1);
    assert_eq!(get_res.kvs()[0].key(), b"c");
    assert_eq!(get_res.kvs()[0].value(), b"bar");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn single_txn_get_after_put_is_ok() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let mut client: KvClient = cluster.client().await.kv_client();

    let tnx_a = Txn::new()
        .when([])
        .and_then([TxnOp::put("f", "foo", None), TxnOp::get("f", None)]);
    let res = client.txn(tnx_a).await?;
    let TxnOpResponse::Get(ref resp) = res.op_responses()[1] else { panic!("invalid response") };
    assert_eq!(resp.kvs()[0].value(), b"foo");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn single_txn_get_after_delete_is_ok() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let mut client: KvClient = cluster.client().await.kv_client();

    client.put("b", "bar", None).await?;

    let tnx_a = Txn::new()
        .when([])
        .and_then([TxnOp::delete("b", None), TxnOp::get("b", None)]);
    let res = client.txn(tnx_a).await?;
    let TxnOpResponse::Get(ref resp) = res.op_responses()[1] else { panic!("invalid response") };
    assert!(resp.kvs().is_empty());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn nested_txn_compare_value_is_ok() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let mut client: KvClient = cluster.client().await.kv_client();

    let txn_inner = Txn::new()
        .when([Compare::value("a", CompareOp::NotEqual, "bar")])
        .or_else([TxnOp::put("b", "baz", None)]);
    let tnx_a = Txn::new()
        .when([])
        .and_then([TxnOp::put("a", "bar", None), TxnOp::txn(txn_inner)]);
    let res = client.txn(tnx_a).await?;
    let TxnOpResponse::Txn(ref resp) = res.op_responses()[1] else { panic!("invalid response") };

    assert!(!resp.succeeded());
    let resp = client.get("b", None).await?;
    assert_eq!(resp.kvs().first().unwrap().value(), b"baz");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn nested_txn_compare_rev_is_ok() -> Result<(), Box<dyn Error>> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "debug");
    }
    _ = tracing_subscriber::fmt()
        .compact()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let mut client: KvClient = cluster.client().await.kv_client();
    client.put("a", "foo", None).await?;
    client.put("a", "foo1", None).await?;

    let txn_inner_a = Txn::new()
        .when([
            Compare::create_revision("a", CompareOp::Equal, 2),
            Compare::mod_revision("a", CompareOp::Equal, 4),
            Compare::version("a", CompareOp::Equal, 3),
            Compare::create_revision("b", CompareOp::Equal, 4),
            Compare::mod_revision("b", CompareOp::Equal, 4),
            Compare::version("b", CompareOp::Equal, 1),
        ])
        .and_then([TxnOp::put("c", "cc", None)]);
    let tnx_a = Txn::new().when([]).and_then([
        TxnOp::put("a", "foo2", None),
        TxnOp::put("b", "bar", None),
        TxnOp::txn(txn_inner_a),
    ]);
    let res = client.txn(tnx_a).await?;
    let TxnOpResponse::Txn(ref resp) = res.op_responses()[2] else { panic!("invalid response") };

    assert!(resp.succeeded());
    Ok(())
}

enum CasResult {
    Success(Vec<Vec<u8>>),
    Fail(Vec<u8>),
}

/// Compare and swap
async fn cas_with_reads(
    client: &mut KvClient,
    key: Vec<u8>,
    read_value: Vec<u8>,
    mod_revision: i64,
    read_revision: i64,
    new_value: Vec<u8>,
    extra_reads: Vec<Vec<u8>>,
) -> Result<CasResult, Box<dyn Error>> {
    let txn = Txn::new()
        .when(if read_value.is_empty() {
            vec![Compare::mod_revision(
                key.clone(),
                CompareOp::Less,
                read_revision,
            )]
        } else {
            vec![Compare::mod_revision(
                key.clone(),
                CompareOp::Equal,
                mod_revision,
            )]
        })
        .and_then(
            [TxnOp::put(key.clone(), new_value, None)]
                .into_iter()
                .chain(extra_reads.into_iter().map(|key| TxnOp::get(key, None)))
                .collect::<Vec<_>>(),
        )
        .or_else([TxnOp::get(key, None)]);
    let resp = client.txn(txn).await?;

    if resp.succeeded() {
        let reads = resp
            .op_responses()
            .into_iter()
            .skip(1)
            .map(|op| {
                let TxnOpResponse::Get(resp) = op else { panic!("invalid response") };
                resp.kvs()
                    .first()
                    .map(|kv| kv.value())
                    .unwrap_or(&[])
                    .to_vec()
            })
            .collect();
        Ok(CasResult::Success(reads))
    } else {
        let TxnOpResponse::Get(ref resp) = resp.op_responses()[0] else { panic!("invalid response") };
        Ok(CasResult::Fail(
            resp.kvs()
                .first()
                .map(|kv| kv.value())
                .unwrap_or(&[])
                .to_vec(),
        ))
    }
}

/// Append
async fn txn_append(
    client: &mut KvClient,
    key: Vec<u8>,
    value: u8,
    // allows [append, read] in a single transaction
    extra_reads: Vec<Vec<u8>>,
) -> Result<(Vec<u8>, Vec<Vec<u8>>), Box<dyn Error>> {
    loop {
        let txn_read = Txn::new()
            .when([])
            .and_then([TxnOp::get(key.clone(), None)]);
        let resp_read = client.txn(txn_read).await.unwrap();
        let TxnOpResponse::Get(ref resp) = resp_read.op_responses()[0] else { panic!("invalid response") };
        let (old_value, mod_revision) = resp
            .kvs()
            .first()
            .map(|kv| (kv.value().to_vec(), kv.mod_revision()))
            .unwrap_or((vec![], 0));
        let old_revision = resp.header().unwrap().revision();
        let append_value: Vec<_> = old_value.clone().into_iter().chain([value]).collect();

        let cas_result = cas_with_reads(
            client,
            key.clone(),
            old_value.clone(),
            mod_revision,
            old_revision,
            append_value.clone(),
            extra_reads.clone(),
        )
        .await
        .unwrap();
        if let CasResult::Success(values) = cas_result {
            return Ok((append_value, values));
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn txn_cas_operation_is_atomic() -> Result<(), Box<dyn Error>> {
    const NUM_CLIENTS: usize = 10;
    const NUM_PER_VALUE: usize = 4;
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let mut client: KvClient = cluster.new_client().await.kv_client();
    let key = vec![1];
    client.put(key.clone(), [0], None).await?;
    let mut handles = vec![];
    let mut clients = vec![];
    for _ in 1..=NUM_CLIENTS {
        let client: KvClient = cluster.new_client().await.kv_client();
        clients.push(client);
    }
    for (i, mut client) in (1..=NUM_CLIENTS).zip(clients) {
        let key = key.clone();
        let handle = tokio::spawn(async move {
            for _ in 0..NUM_PER_VALUE {
                let _val = txn_append(&mut client, key.clone(), i as u8, vec![])
                    .await
                    .unwrap();
            }
        });
        handles.push(handle);
    }
    join_all(handles).await;

    let resp = client.get(key, None).await?;
    assert_eq!(
        resp.kvs().first().unwrap().value().len() - 1,
        NUM_CLIENTS * NUM_PER_VALUE
    );

    Ok(())
}

// Similar to Jepsen's etcd append test
#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn txn_append_is_ok() -> Result<(), Box<dyn Error>> {
    _ = tracing_subscriber::fmt()
        .compact()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    const NUM_CLIENTS: usize = 100;

    let mut cluster = Cluster::new(3).await;
    cluster.start().await;

    let key = vec![1];
    let mut handles = vec![];
    let mut clients = vec![];
    for _ in 1..=NUM_CLIENTS {
        let client: KvClient = cluster.new_client().await.kv_client();
        clients.push(client);
    }

    for (i, mut client) in (1..=NUM_CLIENTS).zip(clients) {
        let key = key.clone();
        let handle = tokio::spawn(async move {
            let (append_value, read_values) =
                txn_append(&mut client, key.clone(), i as u8, vec![key.clone()])
                    .await
                    .unwrap();
            assert_eq!(append_value, read_values[0]);
            read_values.into_iter().next().unwrap()
        });
        handles.push(handle);
    }
    let mut update_values = join_all(handles)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    update_values.sort_unstable();
    let is_last_prefix = |(x, y): (&Vec<u8>, &Vec<u8>)| {
        x.len() + 1 == y.len() && x.iter().zip(y.iter()).all(|(xx, yy)| xx == yy)
    };
    assert!(update_values
        .iter()
        .zip(update_values.iter().skip(1))
        .all(is_last_prefix));

    let mut client = cluster.client().await.kv_client();
    let resp = client.get(key, None).await?;
    assert_eq!(resp.header().unwrap().revision() as usize, 1 + NUM_CLIENTS);

    Ok(())
}

// Similar to Jepsen's etcd append test
#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn txn_append_two_key_is_ok() -> Result<(), Box<dyn Error>> {
    _ = tracing_subscriber::fmt()
        .compact()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    const NUM_CLIENTS: usize = 100;

    let mut cluster = Cluster::new(3).await;
    cluster.start().await;

    let key_a = vec![1];
    let key_b = vec![2];
    let mut handles = vec![];
    let mut clients = vec![];
    for _ in 1..=NUM_CLIENTS {
        let client: KvClient = cluster.new_client().await.kv_client();
        clients.push(client);
    }

    for (i, mut client) in (1..=NUM_CLIENTS).zip(clients) {
        let key_a = key_a.clone();
        let key_b = key_b.clone();
        let handle = tokio::spawn(async move {
            let (append_value_a, read_values_a) =
                txn_append(&mut client, key_a.clone(), i as u8, vec![key_a.clone()])
                    .await
                    .unwrap();
            assert_eq!(append_value_a, read_values_a[0]);
            let (append_value_b, read_values_b) =
                txn_append(&mut client, key_b.clone(), i as u8, vec![key_b.clone()])
                    .await
                    .unwrap();
            assert_eq!(append_value_b, read_values_b[0]);

            (
                read_values_a.into_iter().next().unwrap(),
                read_values_b.into_iter().next().unwrap(),
            )
        });
        handles.push(handle);
    }
    let (mut update_values_a, mut update_values_b): (Vec<_>, Vec<_>) = join_all(handles)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .unzip();
    update_values_a.sort_unstable();
    update_values_b.sort_unstable();
    let is_last_prefix = |(x, y): (&Vec<u8>, &Vec<u8>)| {
        x.len() + 1 == y.len() && x.iter().zip(y.iter()).all(|(xx, yy)| xx == yy)
    };
    assert!(update_values_a
        .iter()
        .zip(update_values_a.iter().skip(1))
        .all(is_last_prefix));
    assert!(update_values_b
        .iter()
        .zip(update_values_b.iter().skip(1))
        .all(is_last_prefix));

    Ok(())
}
