use std::{io, path::PathBuf, task::Poll};

use clippy_utilities::OverflowArithmetic;
use event_listener::Event;
use flume::r#async::RecvStream;
use futures::{FutureExt, StreamExt};
use tokio::task::JoinHandle;
use tokio_stream::Stream;
use tracing::error;

use super::util::LockedFile;

const TEMP_FILE_EXT: &'static str = ".tmp";

pub(super) struct FilePipeline {
    dir: PathBuf,
    file_size: u64,
    file_stream: RecvStream<'static, LockedFile>,
    stop_event: Event,
    handle: JoinHandle<io::Result<()>>,
}

impl FilePipeline {
    /// Creates a new `FilePipeline`
    pub(super) fn new(dir: PathBuf, file_size: u64) -> Self {
        let (file_tx, file_rx) = flume::bounded(1);
        let stop_event = Event::new();
        let mut stop_listener = stop_event.listen();
        let dir_c = dir.clone();

        let handle = tokio::spawn(async move {
            let mut file_count = 0;
            loop {
                let file = Self::alloc(&dir_c, file_size, &mut file_count)?;
                tokio::select! {
                    _ = &mut stop_listener => {
                        break;
                    }
                    // The receiver is already dropped, stop this task
                    Err(_) = file_tx.send_async(file) => {
                        break;
                    }
                }
            }
            Self::clean_up(&dir_c)?;
            Ok(())
        });
        Self {
            dir,
            file_size,
            file_stream: file_rx.into_stream(),
            stop_event,
            handle,
        }
    }

    pub(super) fn stop(&self) {
        self.stop_event.notify(1);
    }

    fn alloc(dir: &PathBuf, file_size: u64, file_count: &mut usize) -> io::Result<LockedFile> {
        let fpath = dir.join(format!("{}{TEMP_FILE_EXT}", *file_count % 2));
        let mut locked_file = LockedFile::open_read_append(fpath)?;
        locked_file.preallocate(file_size)?;
        *file_count = file_count.overflow_add(1);
        Ok(locked_file)
    }

    fn clean_up(dir: &PathBuf) -> io::Result<()> {
        for result in std::fs::read_dir(dir)? {
            let file = result?;
            if let Some(filename) = file.file_name().to_str() {
                if filename.ends_with(TEMP_FILE_EXT) {
                    std::fs::remove_file(file.path())?;
                }
            }
        }
        Ok(())
    }
}

impl Stream for FilePipeline {
    type Item = LockedFile;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.handle.is_finished() {
            if let Poll::Ready(result) = self.handle.poll_unpin(cx) {
                match result {
                    Ok(Err(e)) => {
                        error!("IO error occured in background pipline task: {e}");
                    }
                    Err(e) => {
                        error!("failed to join background pipline task: {e}");
                    }
                    Ok(Ok(_)) => {}
                }
            }
            return std::task::Poll::Ready(None);
        }

        self.file_stream.poll_next_unpin(cx)
    }
}

impl std::fmt::Debug for FilePipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilePipeline")
            .field("dir", &self.dir)
            .field("file_size", &self.file_size)
            .finish()
    }
}
