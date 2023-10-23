use std::{error::Error, time::Duration};

use etcd_client::{Client, Compare, CompareOp, GetOptions, KvClient, Txn, TxnOp, TxnOpResponse};
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

    for test in tests {
        let res = client.range(test.req).await?;
        assert_eq!(res.kvs.len(), test.want_kvs.len());
        let is_identical = res
            .kvs
            .iter()
            .zip(test.want_kvs.iter())
            .all(|(kv, want)| kv.key == want.as_bytes());
        assert!(is_identical);
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
