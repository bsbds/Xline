use std::io::Write;

use criterion::{criterion_group, criterion_main, Criterion};

fn bench(c: &mut Criterion) {
    // NOTE: don't run this under /tmp
    let mut file = tempfile::tempfile_in("./").unwrap();
    let buf = vec![1; 12800];
    c.bench_function("file_write_fsync", |b| {
        b.iter(|| {
            file.write_all(&buf).unwrap();
            file.sync_data().unwrap();
        })
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
