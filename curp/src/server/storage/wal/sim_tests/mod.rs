mod power_failure;

mod silent_corruption;

use std::{
    collections::HashSet,
    env, fs, iter,
    ops::Mul,
    path::{Path, PathBuf},
    pin::Pin,
    process::Command,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use curp_test_utils::test_cmd::TestCommand;
use futures::FutureExt;
use rand::{
    distributions::{uniform::SampleUniform, Uniform},
    random,
    seq::SliceRandom,
    thread_rng, Rng,
};
use tokio::time::interval;
use tracing::info;

use super::{test_util::EntryGenerator, util::get_file_paths_with_ext, *};

const LAZYFS_FIFO_PATH: &str = "/tmp/faults.fifo";

const LAZYFS_MOUTN_POINT: &str = "/tmp/lazyfs.mnt";

const TEST_SEGMENT_SIZE: u64 = 512 * 1024;

#[derive(Clone, Copy)]
struct TestConfig {
    /// Count of the recovery times
    wal_recovery_count: usize,
    /// The running time per recovery
    log_persistent_duration: Duration,
    /// Interval between each logs batch sent
    log_send_interval: Duration,
    /// Interval between each two fault injection (randomized)
    fault_injection_interval: Duration,
    /// Base interval between each head tuncation (randomized)
    head_truncation_interval: Option<Duration>,
    /// Base interval between each tail tuncation (randomized)
    tail_truncation_interval: Option<Duration>,
    /// Percentage of how many bits of the wal segment file will flip
    bitflip_spray_percentage: f64,
}

impl TestConfig {
    fn new_power_failure() -> Self {
        Self {
            wal_recovery_count: 20,
            log_persistent_duration: Duration::from_secs(1),
            log_send_interval: Duration::from_millis(10),
            fault_injection_interval: Duration::from_millis(1000),
            head_truncation_interval: Some(Duration::from_millis(500)),
            tail_truncation_interval: Some(Duration::from_millis(500)),
            bitflip_spray_percentage: 0.01,
        }
    }

    fn new_silent_corruption() -> Self {
        Self {
            wal_recovery_count: 4,
            log_persistent_duration: Duration::from_secs(5),
            log_send_interval: Duration::from_millis(10),
            fault_injection_interval: Duration::from_millis(1000),
            head_truncation_interval: Some(Duration::from_millis(500)),
            tail_truncation_interval: Some(Duration::from_millis(500)),
            bitflip_spray_percentage: 0.01,
        }
    }
}

struct LazyFS;

struct BitFlip {
    path: PathBuf,
}

enum Faults {
    DropUnsync,
    BitFlip,
}

impl LazyFS {
    async fn clear_cache() {
        let start = tokio::time::Instant::now();
        tokio::time::sleep(randomize(Duration::from_millis(1))).await;
        Self::write_fifo("lazyfs::clear-cache").await;
        warn!("LazyFS cache clear completed in: {:?}", start.elapsed());
    }

    async fn checkpoint() {
        Self::write_fifo("lazyfs::cache-checkpoint").await;
    }

    async fn usage() {
        Self::write_fifo("lazyfs::display-cache-usage").await;
    }

    async fn report() {
        Self::write_fifo("lazyfs::unsynced-data-report").await;
    }

    async fn write_fifo(data: &str) {
        tokio::fs::write(LAZYFS_FIFO_PATH, format!("{data}\n"))
            .await
            .unwrap();
    }
}

impl BitFlip {
    fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().into(),
        }
    }

    fn offset(&self, offset: usize) {
        self.spawn_bitflip(vec!["offset", &format!("{offset}")]);
    }

    fn random(&self) {
        self.spawn_bitflip(vec!["random"]);
    }

    fn spray(&self, percent: f64) {
        self.spawn_bitflip(vec!["spray", &format!("percent:{percent:.2}")]);
    }

    fn spawn_bitflip(&self, args: Vec<&str>) {
        let path_str = self.path.as_path().to_str().unwrap();
        let mut c = Command::new("bitflip")
            .args(args.into_iter().chain(iter::once(path_str)))
            .spawn()
            .unwrap();
        c.wait().unwrap();
    }
}

fn init_wal_dir() -> PathBuf {
    let mut wal_dir = PathBuf::from(LAZYFS_MOUTN_POINT);
    wal_dir.push("wal_data");
    fs::remove_dir_all(&wal_dir);
    fs::create_dir(&wal_dir);
    let dir = fs::File::open(&wal_dir).unwrap();
    dir.sync_all().unwrap();
    wal_dir
}

async fn recover_wal_storage(
    wal_dir: impl AsRef<Path>,
) -> io::Result<(WALStorage<TestCommand>, Vec<LogEntry<TestCommand>>)> {
    let config = WALConfig::new(wal_dir, TEST_SEGMENT_SIZE);
    WALStorage::<TestCommand>::new_or_recover(config.clone()).await
}

async fn log_persistent_task(
    mut wal_storage: WALStorage<TestCommand>,
    mut entry_gen: Arc<EntryGenerator>,
    shutdown_flag: Arc<AtomicBool>,
    log_persistent_interval: Duration,
    head_truncation_interval: Option<Duration>,
    tail_truncation_interval: Option<Duration>,
) -> LogIndex {
    // The minimum index that guaranteed to be persistent
    let mut min_persistent_index = 0;
    let mut log_persistent_interval = interval(log_persistent_interval);
    let mut head_truncation_interval = head_truncation_interval.map(randomize).map(interval);
    let mut tail_truncation_interval = tail_truncation_interval.map(randomize).map(interval);

    let mut last_head_truncation_index = 0;

    loop {
        let head_truncation_fut: Pin<Box<dyn Future<Output = ()> + Send>> =
            match head_truncation_interval {
                Some(ref mut i) => Box::pin(i.tick().map(|_| ())),
                None => Box::pin(futures::future::pending::<()>()),
            };
        let tail_truncation_fut: Pin<Box<dyn Future<Output = ()> + Send>> =
            match tail_truncation_interval {
                Some(ref mut i) => Box::pin(i.tick().map(|_| ())),
                None => Box::pin(futures::future::pending::<()>()),
            };

        tokio::select! {
            _ = log_persistent_interval.tick() => {
                let num_per_page = entry_gen.num_entries_per_page();
                let batch_size = randomize(num_per_page as u32) as usize + 1;
                let entries = entry_gen.take(batch_size);

                if shutdown_flag.load(Ordering::SeqCst) {
                    // drop cache on shutdown
                    let handle = tokio::spawn(LazyFS::clear_cache());
                    let start = tokio::time::Instant::now();
                    wal_storage
                        .send_sync(entries.iter().collect())
                        .await
                        .unwrap();
                    info!("Entries persistent completed in: {:?}", start.elapsed());
                    handle.await;
                    break;
                } else {
                    let last_index = entries.last().unwrap().index;
                    wal_storage
                        .send_sync(entries.iter().collect())
                        .await
                        .unwrap();
                    // Here's a checkpoint:
                    // The send_sync successfully completed means that the logs are guaranteed
                    // to be persistented if there's no silent corruption
                    min_persistent_index = last_index;
                }
            }
            _ = head_truncation_fut => {
                let current_index = entry_gen.current_index();
                let range = (last_head_truncation_index + 1)..current_index;
                if range.is_empty() {
                    continue;
                }
                let truncate_index = {
                    let mut rng = thread_rng();
                    rng.gen_range(range)
                };
                warn!("event: head truncation at index: {truncate_index}, current index: {current_index}");
                wal_storage.truncate_head(truncate_index).await.unwrap();
                last_head_truncation_index = truncate_index;
            }
            _ = tail_truncation_fut => {
                let current_index = entry_gen.current_index();
                let range = (last_head_truncation_index + 1)..current_index;
                if range.is_empty() {
                    continue;
                }
                let truncate_index = {
                    let mut rng = thread_rng();
                    rng.gen_range(range)
                };
                warn!("event: tail truncation at index: {truncate_index}, current index: {current_index}");
                wal_storage.truncate_tail(truncate_index).await.unwrap();
                entry_gen.reset_next_index_to(truncate_index + 1);
                min_persistent_index = truncate_index;
            }

        }
    }

    min_persistent_index
}

async fn faults_injection_task(
    wal_dir: impl AsRef<Path>,
    failure_duration: Duration,
    bitflip_spray_percentage: f64,
    shutdown_flag: Arc<AtomicBool>,
    faults: Vec<Faults>,
) {
    let mut interval = interval(failure_duration);
    loop {
        if shutdown_flag.load(Ordering::Acquire) {
            break;
        }
        interval.tick().await;
        let choose = {
            let mut rng = thread_rng();
            faults.choose(&mut rng)
        };
        if let Some(fault) = choose {
            match *fault {
                Faults::DropUnsync => {
                    warn!("fault: clearing LazyFS cache");
                    LazyFS::clear_cache().await;
                }
                Faults::BitFlip => {
                    let wal_paths = get_file_paths_with_ext(wal_dir.as_ref(), ".wal").unwrap();
                    let mut rng = thread_rng();
                    let Some(to_flip) = wal_paths.choose(&mut rng) else { continue; };
                    warn!("fault: flipping the bits of file: {to_flip:?}");
                    let bitflip = BitFlip::new(to_flip);
                    bitflip.spray(bitflip_spray_percentage);
                }
            }
        }
    }
}

fn init_logger() {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "debug");
    }
    _ = tracing_subscriber::fmt()
        .compact()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
}

fn logs_continuety(logs: &Vec<LogEntry<TestCommand>>) -> bool {
    logs.iter()
        .zip(logs.iter().skip(1))
        .all(|(x, y)| x.index + 1 == y.index)
}

fn logs_expect(expected: &Vec<LogEntry<TestCommand>>, recovered: &Vec<LogEntry<TestCommand>>) {
    if recovered.len() == 0 {
        return;
    }
    assert!(logs_continuety(expected));
    assert!(logs_continuety(recovered));
    let start_index = recovered.first().unwrap().index;
    let expected = expected.into_iter().skip_while(|e| e.index < start_index);
    for (e, r) in expected.zip(recovered.into_iter()) {
        assert_eq!(e, r, "recovered logs not match with expected logs");
    }
}

fn randomize<T>(t: T) -> T
where
    T: Default + Mul<u32, Output = T> + Sized + SampleUniform,
{
    let mut rng = thread_rng();
    let uni = Uniform::new(T::default(), t.mul(8));
    rng.sample(uni)
}
