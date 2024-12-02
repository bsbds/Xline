use criterion::{criterion_group, criterion_main, Criterion};
use curp::server::RawCurp;
use curp_test_utils::test_cmd::TestCommand;
use rand::Rng;

fn new_command_with_size(n: usize) -> TestCommand {
    let num_u32 = n / 4;
    let mut rng = rand::thread_rng();
    let key = (0..num_u32).map(|_| rng.gen()).collect();
    TestCommand::new_put(key, 0)
}

fn bench(c: &mut Criterion) {
    // NOTE: don't run this under /tmp
    let dir = tempfile::tempdir_in("./").unwrap();
    const NUM_CMDS: usize = 100;
    const CMD_KEY_SIZE_IN_BYTES: usize = 128;
    let curp = RawCurp::new_bench(dir.path().to_owned());
    let cmds: Vec<_> = std::iter::repeat_with(|| new_command_with_size(CMD_KEY_SIZE_IN_BYTES))
        .take(NUM_CMDS)
        .collect();
    c.bench_function("handle_mutative", |b| {
        b.iter(|| curp.bench_persistent_sp_entries(cmds.clone()))
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
