//! Output plugins. An [`Output`] is shared by every worker; each worker opens
//! its own [`Writer`] so hot paths need no locking unless the sink itself is
//! single-stream (stdout, a file).

use std::collections::HashMap;
use std::sync::Arc;

use crate::conf::StanzaConf;
use crate::envelope::EventOut;

pub mod counter;
pub mod devnull;
pub mod file;
#[cfg(feature = "http")]
pub mod httpevent;
pub mod modinput;
pub mod stdout;
pub mod stoker;

pub trait Writer: Send {
    fn write_batch(&mut self, events: &[EventOut]) -> anyhow::Result<()>;
    fn flush(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

pub trait Output: Send + Sync {
    fn name(&self) -> &str;
    fn open_writer(&self, worker: usize) -> anyhow::Result<Box<dyn Writer>>;
    /// Called once after every writer has flushed.
    fn finish(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Options that come from the command line / environment rather than the conf.
#[derive(Debug, Clone)]
pub struct OutputOptions {
    pub socket_path: String,
    pub socket_connections: SocketConnections,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SocketConnections {
    /// One connection per worker thread (the agent accepts them concurrently).
    PerThread,
    /// One shared connection guarded by a mutex (the Python plugin's shape;
    /// the default, since the agent's per-connection reader threads compete
    /// with its HEC senders for the GIL).
    Single,
}

/// Every output a run needs, keyed by the `outputMode` name (the file output
/// is keyed per file name since each stanza may write its own).
pub struct Outputs {
    map: HashMap<String, Arc<dyn Output>>,
}

impl Outputs {
    pub fn build(samples: &[StanzaConf], opts: &OutputOptions) -> anyhow::Result<Outputs> {
        let mut map: HashMap<String, Arc<dyn Output>> = HashMap::new();
        for s in samples {
            let key = output_key(s);
            if map.contains_key(&key) {
                continue;
            }
            let out: Arc<dyn Output> = match s.output_mode() {
                "stoker" => Arc::new(stoker::StokerOutput::new(&opts.socket_path, opts.socket_connections)),
                "stdout" => Arc::new(stdout::StdoutOutput::new()),
                "devnull" => Arc::new(devnull::DevNullOutput),
                "counter" => Arc::new(counter::CounterOutput::new()),
                "file" => Arc::new(file::FileOutput::new(s)?),
                "modinput" => Arc::new(modinput::ModinputOutput::new()),
                #[cfg(feature = "http")]
                "httpevent" => Arc::new(httpevent::HttpEventOutput::new(s)?),
                other => {
                    log::warn!("outputMode '{}' is not supported by firebox; writing to stdout instead", other);
                    Arc::new(stdout::StdoutOutput::new())
                }
            };
            map.insert(key, out);
        }
        Ok(Outputs { map })
    }

    pub fn get(&self, s: &StanzaConf) -> Arc<dyn Output> {
        self.map.get(&output_key(s)).cloned().expect("output built for every stanza")
    }

    pub fn finish_all(&self) -> anyhow::Result<()> {
        for o in self.map.values() {
            o.finish()?;
        }
        Ok(())
    }
}

fn output_key(s: &StanzaConf) -> String {
    match s.output_mode() {
        "file" => format!("file:{}", s.get("fileName").unwrap_or("")),
        other => other.to_string(),
    }
}

/// Encode a batch as newline-terminated raw lines (stdout/file/devnull).
pub fn encode_raw_lines(events: &[EventOut], buf: &mut Vec<u8>) {
    buf.clear();
    for e in events {
        buf.extend_from_slice(e.raw.as_bytes());
        buf.push(b'\n');
    }
}
