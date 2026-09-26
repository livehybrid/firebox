//! `outputMode = file`: append raw events to `fileName`, rotating at
//! `fileMaxBytes` with `fileBackupFiles` numbered backups
//! (`plugins/output/file.py`).

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;

use super::{Output, Writer};
use crate::conf::StanzaConf;
use crate::envelope::EventOut;

struct State {
    file: File,
    len: u64,
}

struct Inner {
    path: PathBuf,
    max_bytes: u64,
    backups: u32,
    state: Mutex<State>,
}

impl Inner {
    fn rotate(&self, st: &mut State) -> anyhow::Result<()> {
        st.file.flush()?;
        let suffixed = |n: u32| PathBuf::from(format!("{}.{}", self.path.display(), n));
        if self.backups > 0 {
            let last = suffixed(self.backups);
            if last.exists() {
                std::fs::remove_file(&last)?;
            }
            for x in (1..self.backups).rev() {
                let from = suffixed(x);
                if from.exists() {
                    std::fs::rename(&from, suffixed(x + 1))?;
                }
            }
            std::fs::rename(&self.path, suffixed(1))?;
        } else {
            std::fs::remove_file(&self.path)?;
        }
        st.file = OpenOptions::new().create(true).write(true).truncate(true).open(&self.path)?;
        st.len = 0;
        Ok(())
    }
}

pub struct FileOutput {
    inner: Arc<Inner>,
}

impl FileOutput {
    pub fn new(s: &StanzaConf) -> anyhow::Result<FileOutput> {
        let name = s
            .get("fileName")
            .filter(|v| !v.trim().is_empty())
            .ok_or_else(|| anyhow::anyhow!("outputMode file but fileName not specified for sample {}", s.name))?;
        let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        let path = crate::token::path_parser(name, &cwd);
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let len = file.metadata().map(|m| m.len()).unwrap_or(0);
        Ok(FileOutput {
            inner: Arc::new(Inner {
                path,
                max_bytes: s.get_i64("fileMaxBytes").unwrap_or(10_485_760).max(1) as u64,
                backups: s.get_i64("fileBackupFiles").unwrap_or(5).max(0) as u32,
                state: Mutex::new(State { file, len }),
            }),
        })
    }
}

impl Output for FileOutput {
    fn name(&self) -> &str {
        "file"
    }

    fn open_writer(&self, _worker: usize) -> anyhow::Result<Box<dyn Writer>> {
        Ok(Box::new(FileWriter { inner: self.inner.clone(), buf: Vec::with_capacity(256 * 1024) }))
    }

    fn finish(&self) -> anyhow::Result<()> {
        self.inner.state.lock().file.flush()?;
        Ok(())
    }
}

pub struct FileWriter {
    inner: Arc<Inner>,
    buf: Vec<u8>,
}

impl Writer for FileWriter {
    fn write_batch(&mut self, events: &[EventOut]) -> anyhow::Result<()> {
        let inner = &*self.inner;
        let mut st = inner.state.lock();
        self.buf.clear();
        for e in events {
            if e.raw.is_empty() {
                continue;
            }
            let msg_len = e.raw.len() as u64 + 1;
            if st.len + (self.buf.len() as u64) + msg_len > inner.max_bytes {
                if !self.buf.is_empty() {
                    st.file.write_all(&self.buf)?;
                    st.len += self.buf.len() as u64;
                    self.buf.clear();
                }
                if st.len + msg_len > inner.max_bytes {
                    inner.rotate(&mut st)?;
                }
            }
            self.buf.extend_from_slice(e.raw.as_bytes());
            self.buf.push(b'\n');
        }
        if !self.buf.is_empty() {
            st.file.write_all(&self.buf)?;
            st.len += self.buf.len() as u64;
        }
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        self.inner.state.lock().file.flush()?;
        Ok(())
    }
}
