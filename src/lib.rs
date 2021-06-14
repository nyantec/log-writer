mod fsstats;

use std::os::unix::fs::MetadataExt;
use std::io::{Result, Write, BufWriter};
use std::fs;
use std::path::PathBuf;
use log::{info, warn};
use chrono::Local;

pub struct LogWriterConfig {
    pub target_dir: PathBuf,
    pub prefix: String,
    pub suffix: String,
    /// The maximum amount of space that is allowed to be used,
    /// relative to the total space (0.01 = 1%)
    pub max_use_of_total: Option<f64>,
    /// The minimum amount of space that should be kept available at all times,
    /// relative to the total space (0.01 = 1%)
    pub min_avail_of_total: Option<f64>,
    /// The minimum amount of space that should be kept available at all times,
    /// in bytes
    pub min_avail_bytes: Option<usize>,
    pub max_file_size: usize,
}

pub struct LogWriter {
    cfg: LogWriterConfig,
    current: BufWriter<fs::File>,
    current_name: String,
    current_size: usize,
}

fn create_next_file(cfg: &LogWriterConfig) -> Result<(String, BufWriter<fs::File>)> {
    let name = format!("{}{}{}", cfg.prefix, Local::now().format("%Y-%m-%d"), cfg.suffix);
    let file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(cfg.target_dir.join(&name))?;
    Ok((name, BufWriter::new(file)))
} 

impl LogWriter {
    pub fn new(cfg: LogWriterConfig) -> Result<Self> {
        let (current_name, current) = create_next_file(&cfg)?;
        Ok(Self {
            cfg,
            current_name,
            current,
            current_size: 0,
        })
    }

    fn enough_space(&mut self, len: usize) -> Result<bool> {
        let fsstat = fsstats::statvfs(&self.cfg.target_dir)?;

        if let Some(max_use_of_total) = self.cfg.max_use_of_total {
            let mut used = 0;
            for entry in fs::read_dir(&self.cfg.target_dir)? {
                let entry = match entry {
                    Err(_) => {
                        info!("entry get failed during size calculation");
                        continue;
                    },
                    Ok(entry) => entry,
                };
                let path = entry.path();

                let meta = match entry.metadata() {
                    Err(_) => {
                        info!("could not get metadata for \"{:?}\", ignoring for size calculation", &path);
                        continue;
                    },
                    Ok(meta) => meta,
                };

                if !meta.is_file() {
                    info!("ignoring non-file \"{:?}\" for size calculation", &path);
                    continue;
                }

                used += meta.blocks() * 512;
            };
            let used_of_total = used as f64 / fsstat.total_space as f64;
            if used_of_total > max_use_of_total {
                return Ok(false);
            }
        }

        if let Some(min_avail_of_total) = self.cfg.min_avail_of_total {
            let avail = fsstat.available_space - len as u64;
            let avail_of_total = avail as f64 / fsstat.total_space as f64;
            if avail_of_total < min_avail_of_total {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// deletes one file.
    /// returns Ok(true) if a file was deleted.
    /// returns Ok(false) if there was no file to delete.
    fn cleanup(&mut self) -> Result<bool> {
        let mut entries: Vec<_> = fs::read_dir(&self.cfg.target_dir)?
            .filter_map(|x| x.ok())
            .filter(|x| x.file_type().and_then(|t| Ok(t.is_file())).unwrap_or(false))
            .collect();

        entries.sort_by(|a, b| a.path().cmp(&b.path()));

        let oldest_file = match entries.get(0) {
            None => return Ok(false),
            Some(file) => file,
        };

        fs::remove_file(oldest_file.path())?;
        Ok(true)
    }

    fn next_file(&mut self) -> Result<()> {
        let (next_name, next) = create_next_file(&self.cfg)?;
        self.current_name = next_name;
        self.current = next;
        Ok(())
    }
}

impl Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        if self.current_size + buf.len() > self.cfg.max_file_size {
            self.next_file()?;
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
