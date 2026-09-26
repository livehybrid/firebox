//! The Stoker agent protocol: NDJSON envelopes over a unix stream socket at
//! `STOKER_OUTPUT_SOCKET`. Writes are blocking so a stalled agent stalls
//! generation (that is the backpressure design). A failed write is fatal for
//! the whole engine: the agent restarts it rather than losing events quietly.

use std::io::Write;
use std::os::unix::net::UnixStream;
use std::sync::Arc;

use parking_lot::Mutex;

use super::{Envelope, Output, SocketConnections, Writer};
use crate::envelope::{write_hec_line_resolved, write_stoker_line, EventOut};

pub struct StokerOutput {
    path: String,
    mode: SocketConnections,
    envelope: Envelope,
    shared: Arc<Mutex<Option<UnixStream>>>,
}

impl StokerOutput {
    pub fn new(path: &str, mode: SocketConnections, envelope: Envelope) -> StokerOutput {
        StokerOutput { path: path.to_string(), mode, envelope, shared: Arc::new(Mutex::new(None)) }
    }

    fn connect(&self) -> anyhow::Result<UnixStream> {
        UnixStream::connect(&self.path)
            .map_err(|e| anyhow::anyhow!("cannot connect to agent socket {}: {}", self.path, e))
    }
}

impl Output for StokerOutput {
    fn name(&self) -> &str {
        "stoker"
    }

    fn open_writer(&self, _worker: usize) -> anyhow::Result<Box<dyn Writer>> {
        match self.mode {
            SocketConnections::PerThread => Ok(Box::new(StokerWriter {
                stream: Stream::Own(self.connect()?),
                buf: Vec::with_capacity(256 * 1024),
                envelope: self.envelope.clone(),
            })),
            SocketConnections::Single => {
                let mut guard = self.shared.lock();
                if guard.is_none() {
                    *guard = Some(self.connect()?);
                }
                Ok(Box::new(StokerWriter {
                    stream: Stream::Shared(self.shared.clone()),
                    buf: Vec::with_capacity(256 * 1024),
                    envelope: self.envelope.clone(),
                }))
            }
        }
    }
}

enum Stream {
    Own(UnixStream),
    Shared(Arc<Mutex<Option<UnixStream>>>),
}

pub struct StokerWriter {
    stream: Stream,
    buf: Vec<u8>,
    envelope: Envelope,
}

impl Writer for StokerWriter {
    fn write_batch(&mut self, events: &[EventOut]) -> anyhow::Result<()> {
        self.buf.clear();
        match &self.envelope {
            Envelope::Stoker => {
                for e in events {
                    write_stoker_line(&mut self.buf, e);
                }
            }
            Envelope::Hec(policy) => {
                for e in events {
                    write_hec_line_resolved(&mut self.buf, e, policy);
                }
            }
        }
        if self.buf.is_empty() {
            return Ok(());
        }
        let res = match &mut self.stream {
            Stream::Own(s) => s.write_all(&self.buf),
            Stream::Shared(m) => {
                let mut guard = m.lock();
                match guard.as_mut() {
                    Some(s) => s.write_all(&self.buf),
                    None => Err(std::io::Error::other("agent socket closed")),
                }
            }
        };
        res.map_err(|e| anyhow::anyhow!("agent socket write failed: {}", e))
    }
}
