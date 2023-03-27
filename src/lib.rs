//! A library to write a stream to disk while adhering usage limits.
//! Inspired by journald, but more general-purpose.

mod fsstats;

use chrono::Local;
use log::{info, warn};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::fs;
use std::io::{BufWriter, Error, Result, Write};
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct LogWriterConfig {
    pub target_dir: PathBuf,
    pub prefix: String,
    pub suffix: String,
    /// The maximum amount of space that is allowed to be used,
    /// relative to the total space (0.01 = 1%)
    pub max_use_of_total: Option<f64>,
    /// The maximum amount of space that is allowed to be used,
    /// in bytes
    pub max_use_bytes: Option<u64>,
    /// The minimum amount of space that should be kept available at all times,
    /// relative to the total space (0.01 = 1%)
    pub min_avail_of_total: Option<f64>,
    pub warn_if_avail_reached: bool,
    /// The minimum amount of space that should be kept available at all times,
    /// in bytes
    pub min_avail_bytes: Option<usize>,
    pub max_file_size: usize,
    /// Rotated after X seconds, regardless of size
    pub max_file_age: Option<u64>,
    /// Disk space subtracted when checking if max_use_of_total is reached.
    /// Set this to the absolute amount of space you expect other services to take up on the
    /// partition.
    /// in bytes
    pub reserved: Option<usize>,
}

/// Writes a stream to disk while adhering to the usage limits described in `cfg`.
///
/// When `write()` is called, the LogWriter will attempt to ensure enough space is
/// available to write the new contents. In some cases, where no more space can be
/// freed, `ENOSPC` may be returned.
pub struct LogWriter<T: LogWriterCallbacks + Sized + Clone + Debug> {
    cfg: LogWriterConfig,
    current: BufWriter<fs::File>,
    current_name: String,
    current_size: usize,
    write_start: Instant,
    callbacks: T,
}

pub trait LogWriterCallbacks: Sized + Clone + Debug {
    fn start_file(&mut self, log_writer: &mut LogWriter<Self>) -> Result<()>;
    fn end_file(&mut self, log_writer: &mut LogWriter<Self>) -> Result<()>;
}

#[derive(Clone, Debug)]
pub struct NoopLogWriterCallbacks;
impl LogWriterCallbacks for NoopLogWriterCallbacks {
    fn start_file(&mut self, _log_writer: &mut LogWriter<Self>) -> Result<()> {
        Ok(())
    }
    fn end_file(&mut self, _log_writer: &mut LogWriter<Self>) -> Result<()> {
        Ok(())
    }
}

fn create_next_file(cfg: &LogWriterConfig) -> Result<(String, BufWriter<fs::File>)> {
    let name = format!(
        "{}{}{}",
        cfg.prefix,
        Local::now().format("%Y-%m-%d-%H-%M-%S"),
        cfg.suffix
    );
    let file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(cfg.target_dir.join(&name))?;
    Ok((name, BufWriter::new(file)))
}

impl LogWriter<NoopLogWriterCallbacks> {
    pub fn new(cfg: LogWriterConfig) -> Result<Self> {
        LogWriter::new_with_callbacks(cfg, NoopLogWriterCallbacks)
    }
}

impl<T: LogWriterCallbacks + Sized + Clone + Debug> LogWriter<T> {
    pub fn new_with_callbacks(cfg: LogWriterConfig, callbacks: T) -> Result<Self> {
        fs::create_dir_all(&cfg.target_dir)?;
        let (current_name, current) = create_next_file(&cfg)?;
        let mut log_writer = Self {
            cfg,
            current_name,
            current,
            current_size: 0,
            write_start: Instant::now(),
            callbacks,
        };
        log_writer.callbacks.clone().start_file(&mut log_writer)?;
        Ok(log_writer)
    }

    fn file_listing<'a>(&'a self) -> Result<impl Iterator<Item = (fs::DirEntry, String)> + 'a> {
        let prefix = self.cfg.prefix.clone();
        let suffix = self.cfg.suffix.clone();
        let iter = fs::read_dir(&self.cfg.target_dir)?
            .filter_map(|x| x.ok())
            .filter(|x| x.file_type().and_then(|t| Ok(t.is_file())).unwrap_or(false))
            .filter_map(|file| match file.file_name().into_string() {
                Ok(file_name) => Some((file, file_name)),
                Err(_) => None,
            })
            .filter(move |(_, file_name)| {
                file_name.starts_with(&prefix) && file_name.ends_with(&suffix)
            });
        Ok(iter)
    }

    fn enough_space(&mut self, len: usize) -> Result<bool> {
        let fsstat = fsstats::statvfs(&self.cfg.target_dir)?;

        let mut size_limit = u64::MAX;

        if let Some(max_use_bytes) = self.cfg.max_use_bytes {
            size_limit = std::cmp::min(size_limit, max_use_bytes);
        }

        if let Some(max_use_of_total) = self.cfg.max_use_of_total {
            let mut max_use_bytes = (max_use_of_total * fsstat.total_space as f64) as u64;
            if let Some(reserved) = self.cfg.reserved {
                max_use_bytes -= reserved as u64;
            };
            size_limit = std::cmp::min(size_limit, max_use_bytes);
        }

        if size_limit != u64::MAX {
            let mut used = 0u64;
            for (entry, _) in self.file_listing()? {
                let path = entry.path();

                let meta = match entry.metadata() {
                    Err(_) => {
                        info!(
                            "could not get metadata for \"{:?}\", ignoring for size calculation",
                            &path
                        );
                        continue;
                    }
                    Ok(meta) => meta,
                };

                used += meta.blocks() * 512;
            }
            if used > size_limit {
                return Ok(false);
            }
        }

        if let Some(min_avail_of_total) = self.cfg.min_avail_of_total {
            let avail = fsstat.available_space - len as u64;
            let avail_of_total = avail as f64 / fsstat.total_space as f64;
            if avail_of_total < min_avail_of_total {
                if self.cfg.warn_if_avail_reached {
                    warn!("min_avail_of_total reached, you said this shouldn't happen");
                }
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// deletes one file.
    /// returns Ok(true) if a file was deleted.
    /// returns Ok(false) if there was no file to delete.
    fn cleanup(&mut self) -> Result<bool> {
        let mut entries: Vec<_> = self.file_listing()?.collect();

        entries.sort_by(|(_, a), (_, b)| a.cmp(&b));

        let (oldest_file, file_name) = match entries.get(0) {
            Some(v) => v,
            None => {
                warn!("log-writer can not free space: no files to delete");
                return Err(Error::from_raw_os_error(libc::ENOSPC));
            }
        };

        if *file_name == self.current_name {
            warn!("log-writer can not free space: oldest file is current file");
            return Err(Error::from_raw_os_error(libc::ENOSPC));
        }

        fs::remove_file(oldest_file.path())?;
        Ok(true)
    }

    fn next_file(&mut self) -> Result<()> {
        let (next_name, next) = create_next_file(&self.cfg)?;
        self.callbacks.clone().end_file(self)?;
        self.current.flush()?;
        self.current_name = next_name;
        self.current_size = 0;
        self.write_start = Instant::now();
        self.current = next;
        self.callbacks.clone().start_file(self)?;
        Ok(())
    }
}

impl<T: LogWriterCallbacks + Sized + Clone + Debug> Write for LogWriter<T> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        if self.current_size + buf.len() > self.cfg.max_file_size {
            self.next_file()?;
        }
        if let Some(max_file_age) = self.cfg.max_file_age {
            if Instant::now().duration_since(self.write_start).as_secs() > max_file_age {
                self.next_file()?;
            }
        }

        while !self.enough_space(buf.len())? {
            if !self.cleanup()? {
                warn!("could not free enough space, this might cause strange behaviour");
                break;
            }
        }

        let written = self.current.write(buf)?;
        self.current_size += written;

        Ok(written)
    }

    fn flush(&mut self) -> Result<()> {
        self.current.flush()
    }
}
