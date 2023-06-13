use xlineapi::{KeyValue, ResponseHeader};

/// Printer of common response types
pub(crate) struct Printer;

impl Printer {
    /// Response header printer
    pub(crate) fn header(header: Option<&ResponseHeader>) {
        let Some(header) = header else { return };
        println!("header:");
        println!(
            "cluster_id: {}, member_id: {}, revision: {}, raft_term: {}",
            header.cluster_id, header.member_id, header.revision, header.raft_term
        );
    }

    /// Response key printer
    pub(crate) fn key(key: &[u8]) {
        println!("key: {}", String::from_utf8_lossy(key));
    }

    /// Response key printer
    pub(crate) fn range_end(range_end: &[u8]) {
        println!("range_end: {}", String::from_utf8_lossy(range_end));
    }

    /// Response value printer
    pub(crate) fn value(value: &[u8]) {
        println!("value: {}", String::from_utf8_lossy(value));
    }

    /// Response key-value printer
    pub(crate) fn kv(kv: &KeyValue) {
        Self::key(&kv.key);
        Self::value(&kv.value);
    }
}
