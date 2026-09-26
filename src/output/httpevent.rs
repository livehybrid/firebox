//! `outputMode = httpevent`: Splunk HTTP Event Collector, batched per worker.
//!
//! Settings (as upstream): `httpeventServers` (JSON `{"servers": [{"protocol",
//! "address", "port", "key"}]}`), `httpeventOutputMode` (`roundrobin` |
//! `mirror`), `httpeventMaxPayloadSize` (bytes, default 10000),
//! `httpeventAllowFailureCount` (default 100). Firebox adds `httpeventGzip`
//! (default true).

use std::io::Write;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::{Output, Writer};
use crate::conf::{parse_bool, StanzaConf};
use crate::envelope::{write_hec_object, EventOut};

#[derive(Debug, Clone, serde::Deserialize)]
struct ServerSpec {
    #[serde(default = "default_protocol")]
    protocol: String,
    address: String,
    #[serde(default = "default_port")]
    port: serde_json::Value,
    key: String,
}

fn default_protocol() -> String {
    "https".into()
}

fn default_port() -> serde_json::Value {
    serde_json::Value::from(8088)
}

#[derive(Debug, Clone, serde::Deserialize)]
struct Servers {
    servers: Vec<ServerSpec>,
}

#[derive(Debug, Clone)]
struct Server {
    url: String,
    auth: String,
}

struct Inner {
    servers: Vec<Server>,
    mirror: bool,
    max_payload: usize,
    allow_failures: u64,
    gzip: bool,
    failures: AtomicU64,
    rr: AtomicUsize,
}

pub struct HttpEventOutput {
    inner: Arc<Inner>,
}

impl HttpEventOutput {
    pub fn new(s: &StanzaConf) -> anyhow::Result<HttpEventOutput> {
        let spec =
            s.get("httpeventServers").ok_or_else(|| anyhow::anyhow!("outputMode httpevent needs httpeventServers"))?;
        let parsed: Servers =
            serde_json::from_str(spec).map_err(|e| anyhow::anyhow!("httpeventServers is not valid JSON: {}", e))?;
        if parsed.servers.is_empty() {
            anyhow::bail!("httpeventServers lists no servers");
        }
        let servers = parsed
            .servers
            .into_iter()
            .map(|sv| {
                let port = match &sv.port {
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::String(t) => t.clone(),
                    _ => "8088".into(),
                };
                Server {
                    url: format!("{}://{}:{}/services/collector", sv.protocol, sv.address, port),
                    auth: format!("Splunk {}", sv.key),
                }
            })
            .collect();
        Ok(HttpEventOutput {
            inner: Arc::new(Inner {
                servers,
                mirror: s.get("httpeventOutputMode").map(|m| m.trim().eq_ignore_ascii_case("mirror")).unwrap_or(false),
                max_payload: s.get_i64("httpeventMaxPayloadSize").unwrap_or(10_000).max(1) as usize,
                allow_failures: s.get_i64("httpeventAllowFailureCount").unwrap_or(100).max(0) as u64,
                gzip: s.get("httpeventGzip").map(parse_bool).unwrap_or(true),
                failures: AtomicU64::new(0),
                rr: AtomicUsize::new(0),
            }),
        })
    }
}

impl Output for HttpEventOutput {
    fn name(&self) -> &str {
        "httpevent"
    }

    fn open_writer(&self, _worker: usize) -> anyhow::Result<Box<dyn Writer>> {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(30)))
            .http_status_as_error(false)
            .build()
            .into();
        Ok(Box::new(HttpEventWriter {
            inner: self.inner.clone(),
            agent,
            buf: Vec::with_capacity(self.inner.max_payload + 4096),
        }))
    }

    fn finish(&self) -> anyhow::Result<()> {
        let f = self.inner.failures.load(Ordering::Relaxed);
        if f > 0 {
            log::warn!("httpevent: {} failed POST(s)", f);
        }
        Ok(())
    }
}

pub struct HttpEventWriter {
    inner: Arc<Inner>,
    agent: ureq::Agent,
    buf: Vec<u8>,
}

impl HttpEventWriter {
    fn post(&mut self) -> anyhow::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let inner = &*self.inner;
        let body: Vec<u8> = if inner.gzip {
            let mut enc =
                flate2::write::GzEncoder::new(Vec::with_capacity(self.buf.len() / 2), flate2::Compression::new(6));
            enc.write_all(&self.buf)?;
            enc.finish()?
        } else {
            self.buf.clone()
        };
        let targets: Vec<&Server> = if inner.mirror {
            inner.servers.iter().collect()
        } else {
            let i = inner.rr.fetch_add(1, Ordering::Relaxed) % inner.servers.len();
            vec![&inner.servers[i]]
        };
        for server in targets {
            let mut req = self
                .agent
                .post(&server.url)
                .header("Authorization", &server.auth)
                .header("Content-Type", "application/json");
            if inner.gzip {
                req = req.header("Content-Encoding", "gzip");
            }
            let ok = match req.send(&body[..]) {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    if (200..300).contains(&status) {
                        true
                    } else {
                        log::warn!("httpevent: {} returned HTTP {}", server.url, status);
                        false
                    }
                }
                Err(e) => {
                    log::warn!("httpevent: POST to {} failed: {}", server.url, e);
                    false
                }
            };
            if !ok {
                let n = inner.failures.fetch_add(1, Ordering::Relaxed) + 1;
                if n > inner.allow_failures {
                    anyhow::bail!("httpevent: more than {} failed POSTs; giving up", inner.allow_failures);
                }
            }
        }
        self.buf.clear();
        Ok(())
    }
}

impl Writer for HttpEventWriter {
    fn write_batch(&mut self, events: &[EventOut]) -> anyhow::Result<()> {
        let max_payload = self.inner.max_payload;
        let mut one = Vec::with_capacity(512);
        for e in events {
            one.clear();
            write_hec_object(&mut one, e);
            if !self.buf.is_empty() && self.buf.len() + one.len() + 1 > max_payload {
                self.post()?;
            }
            if !self.buf.is_empty() {
                self.buf.push(b'\n');
            }
            self.buf.extend_from_slice(&one);
        }
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        self.post()
    }
}
