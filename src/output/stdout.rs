//! `outputMode = stdout`: one raw event per line, one write per batch.

use std::io::Write;

use super::{encode_raw_lines, Output, Writer};
use crate::envelope::EventOut;

pub struct StdoutOutput;

impl StdoutOutput {
    pub fn new() -> StdoutOutput {
        StdoutOutput
    }
}

impl Default for StdoutOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl Output for StdoutOutput {
    fn name(&self) -> &str {
        "stdout"
    }

    fn open_writer(&self, _worker: usize) -> anyhow::Result<Box<dyn Writer>> {
        Ok(Box::new(StdoutWriter { buf: Vec::with_capacity(256 * 1024) }))
    }
}

pub struct StdoutWriter {
    buf: Vec<u8>,
}

impl Writer for StdoutWriter {
    fn write_batch(&mut self, events: &[EventOut]) -> anyhow::Result<()> {
        encode_raw_lines(events, &mut self.buf);
        let stdout = std::io::stdout();
        let mut lock = stdout.lock();
        lock.write_all(&self.buf)?;
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        std::io::stdout().lock().flush()?;
        Ok(())
    }
}
