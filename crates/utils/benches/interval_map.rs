#![feature(test)]

extern crate test;
extern crate utils;

use test::Bencher;

use utils::array_interval_map::{Interval, IntervalMap};

struct Rng {
    state: u32,
}

impl Rng {
    fn new() -> Self {
        Self { state: 0x87654321 }
    }

    fn gen_u32(&mut self) -> u32 {
        self.state ^= self.state << 13;
        self.state ^= self.state >> 17;
        self.state ^= self.state << 5;
        self.state
    }

    fn gen_range_i32(&mut self, low: i32, high: i32) -> i32 {
        let d = (high - low) as u32;
        low + (self.gen_u32() % d) as i32
    }
}

struct IntervalGenerator {
    rng: Rng,
    limit: i32,
}

impl IntervalGenerator {
    fn new() -> Self {
        const LIMIT: i32 = 1000;
        Self {
            rng: Rng::new(),
            limit: LIMIT,
        }
    }

    fn next(&mut self) -> Interval<i32> {
        let low = self.rng.gen_range_i32(0, self.limit - 1);
        let high = self.rng.gen_range_i32(low + 1, self.limit);
        Interval::new(low, high)
    }
}

fn bench_interval_map(count: usize, bench: &mut Bencher) {
    let mut gen = IntervalGenerator::new();
    let intervals: Vec<_> = std::iter::repeat_with(|| gen.next()).take(count).collect();
    bench.iter(|| {
        let mut map = IntervalMap::new();
        for i in intervals.clone() {
            map.insert(i, ());
        }
    });
}

#[bench]
fn bench_interval_map_100(bench: &mut Bencher) {
    bench_interval_map(100, bench);
}

#[bench]
fn bench_interval_map_1000(bench: &mut Bencher) {
    bench_interval_map(1000, bench);
}

#[bench]
fn bench_interval_map_10_000(bench: &mut Bencher) {
    bench_interval_map(10_000, bench);
}

#[bench]
fn bench_interval_map_100_000(bench: &mut Bencher) {
    bench_interval_map(100_000, bench);
}
