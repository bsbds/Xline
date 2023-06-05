use std::sync::atomic::{AtomicU64, Ordering};

use clippy_utilities::Cast;

/// Generator of unique lease id
/// id format:
/// | prefix       | suffix  |
/// | 7 bytes      | 1 byte  |
/// | random bytes | cnt     |
#[derive(Debug, Default)]
pub struct LeaseIdGenerator {
    /// prefix of id
    prefix: u64,
    /// suffix of id
    suffix: AtomicU64,
}

impl LeaseIdGenerator {
    /// New `IdGenerator`
    ///
    /// # Panics
    ///
    /// panic if failed to generate random bytes
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        let mut buf = [0u8; 8];
        getrandom::getrandom(&mut buf).unwrap_or_else(|err| {
            panic!("Failed to generate random bytes for lease id generator: {err}");
        });
        let prefix = u64::from_be_bytes(buf).overflowing_shl(1).0;
        let suffix = AtomicU64::new(0);
        Self { prefix, suffix }
    }

    /// Generate next id
    #[inline]
    pub fn next(&self) -> i64 {
        let suffix = self.suffix.fetch_add(1, Ordering::Relaxed);
        let id = self.prefix | suffix;
        (id & 0x7fff_ffff_ffff_ffff).cast()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_id_generator() {
        let id_gen = LeaseIdGenerator::new();
        assert_ne!(id_gen.next(), id_gen.next());
        assert_ne!(id_gen.next(), id_gen.next());
        assert_ne!(id_gen.next(), id_gen.next());
    }
}
