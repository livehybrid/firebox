//! `outputMode = modinput`: the Splunk modular-input XML stream that eventgen
//! emits when embedded in Splunk. Useful standalone to eyeball metadata.

use std::io::Write;

use super::{Output, Writer};
use crate::envelope::{EventOut, TimeVal};

pub struct ModinputOutput;

impl ModinputOutput {
    pub fn new() -> ModinputOutput {
        ModinputOutput
    }
}

impl Default for ModinputOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl Output for ModinputOutput {
    fn name(&self) -> &str {
        "modinput"
    }

    fn open_writer(&self, _worker: usize) -> anyhow::Result<Box<dyn Writer>> {
        Ok(Box::new(ModinputWriter { buf: Vec::with_capacity(256 * 1024) }))
    }
}

pub struct ModinputWriter {
    buf: Vec<u8>,
}

fn xml_escape(out: &mut Vec<u8>, s: &str) {
    for b in s.bytes() {
        match b {
            b'&' => out.extend_from_slice(b"&amp;"),
            b'<' => out.extend_from_slice(b"&lt;"),
            b'>' => out.extend_from_slice(b"&gt;"),
            _ => out.push(b),
        }
    }
}

impl Writer for ModinputWriter {
    fn write_batch(&mut self, events: &[EventOut]) -> anyhow::Result<()> {
        self.buf.clear();
        self.buf.extend_from_slice(b"<stream>");
        for e in events {
            self.buf.extend_from_slice(b"<event>");
            match e.time {
                TimeVal::Int(i) => {
                    self.buf.extend_from_slice(b"<time>");
                    self.buf.extend_from_slice(i.to_string().as_bytes());
                    self.buf.extend_from_slice(b"</time>");
                }
                TimeVal::Float(f) => {
                    self.buf.extend_from_slice(b"<time>");
                    self.buf.extend_from_slice(format!("{}", f).as_bytes());
                    self.buf.extend_from_slice(b"</time>");
                }
                TimeVal::None => {}
            }
            for (tag, val) in [
                ("index", &e.meta.index),
                ("host", &e.meta.host),
                ("source", &e.meta.source),
                ("sourcetype", &e.meta.sourcetype),
            ] {
                if let Some(v) = val {
                    self.buf.push(b'<');
                    self.buf.extend_from_slice(tag.as_bytes());
                    self.buf.push(b'>');
                    xml_escape(&mut self.buf, v);
                    self.buf.extend_from_slice(b"</");
                    self.buf.extend_from_slice(tag.as_bytes());
                    self.buf.push(b'>');
                }
            }
            self.buf.extend_from_slice(b"<data>");
            xml_escape(&mut self.buf, &e.raw);
            self.buf.extend_from_slice(b"</data></event>\n");
        }
        self.buf.extend_from_slice(b"</stream>\n");
        std::io::stdout().lock().write_all(&self.buf)?;
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        std::io::stdout().lock().flush()?;
        Ok(())
    }
}
