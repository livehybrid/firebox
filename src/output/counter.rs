//! `outputMode = counter`: per-day event and byte histograms printed to stderr.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::Mutex;

use super::{Output, Writer};
use crate::envelope::{EventOut, TimeVal};

#[derive(Default)]
struct Histograms {
    bytes: BTreeMap<String, u64>,
    events: BTreeMap<String, u64>,
}

pub struct CounterOutput {
    hist: Arc<Mutex<Histograms>>,
}

impl CounterOutput {
    pub fn new() -> CounterOutput {
        CounterOutput { hist: Arc::new(Mutex::new(Histograms::default())) }
    }

    fn print(&self) {
        let h = self.hist.lock();
        let now = crate::clock::now_local().format("%Y-%m-%d %H:%M:%S%.6f");
        eprintln!("{} ----- print the output histogram -----", now);
        eprintln!("{} --- data size histogram ---", now);
        eprintln!("{} {:?}", now, h.bytes);
        eprintln!("{} --- event count histogram ---", now);
        eprintln!("{} {:?}", now, h.events);
    }
}

impl Default for CounterOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl Output for CounterOutput {
    fn name(&self) -> &str {
        "counter"
    }

    fn open_writer(&self, _worker: usize) -> anyhow::Result<Box<dyn Writer>> {
        Ok(Box::new(CounterWriter { hist: self.hist.clone() }))
    }

    fn finish(&self) -> anyhow::Result<()> {
        self.print();
        Ok(())
    }
}

pub struct CounterWriter {
    hist: Arc<Mutex<Histograms>>,
}

impl Writer for CounterWriter {
    fn write_batch(&mut self, events: &[EventOut]) -> anyhow::Result<()> {
        let mut local: BTreeMap<String, (u64, u64)> = BTreeMap::new();
        for e in events {
            let epoch = match e.time {
                TimeVal::Int(i) => i,
                TimeVal::Float(f) => f as i64,
                TimeVal::None => crate::clock::now_epoch_f64() as i64,
            };
            let day = crate::clock::local_from_epoch(epoch)
                .map(|d| d.format("%Y-%m-%d").to_string())
                .unwrap_or_else(|| "unknown".into());
            let slot = local.entry(day).or_default();
            slot.0 += e.raw.len() as u64;
            slot.1 += 1;
        }
        let mut h = self.hist.lock();
        for (day, (b, n)) in local {
            *h.bytes.entry(day.clone()).or_default() += b;
            *h.events.entry(day).or_default() += n;
        }
        Ok(())
    }
}
