//! `outputMode = devnull`: generate, encode, discard. Used by `firebox bench`.

use super::{encode_raw_lines, Output, Writer};
use crate::envelope::EventOut;

pub struct DevNullOutput;

impl Output for DevNullOutput {
    fn name(&self) -> &str {
        "devnull"
    }

    fn open_writer(&self, _worker: usize) -> anyhow::Result<Box<dyn Writer>> {
        Ok(Box::new(DevNullWriter { buf: Vec::with_capacity(64 * 1024) }))
    }
}

pub struct DevNullWriter {
    buf: Vec<u8>,
}

impl Writer for DevNullWriter {
    fn write_batch(&mut self, events: &[EventOut]) -> anyhow::Result<()> {
        // Encoding keeps the benchmark honest: the work of producing the bytes
        // is done, only the syscall is skipped.
        encode_raw_lines(events, &mut self.buf);
        std::hint::black_box(&self.buf);
        Ok(())
    }
}
