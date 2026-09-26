//! Generation plans (which lines an interval emits) and shared helpers.
//! The per-event work lives in `engine::run_job`; replay in `gen::replay`.

use rand::rngs::SmallRng;
use rand::Rng;

use crate::engine::{Generator, Plan, Sample, CHUNK_BYTES, CHUNK_EVENTS};
use crate::rate::RateMaps;
use crate::strftime::Ts;
use crate::token::EventCtx;

pub mod replay;

/// Events a plan will produce (byte plans are estimated from the mean size).
pub fn plan_events(p: &Plan) -> usize {
    match p {
        Plan::Lines { count, .. } | Plan::RandomLines { count } => *count,
        Plan::Bundle { copies } => *copies,
        Plan::RandomBytes { .. } => 0,
        Plan::Windbag { count, .. } | Plan::Counter { count, .. } => *count,
    }
}

/// Plans for a count-driven interval (default, windbag, counter generators).
pub fn plan_count(s: &Sample, count: i64) -> Vec<Plan> {
    let n = s.data.len();
    let mut out = Vec::new();
    match s.generator {
        Generator::Windbag | Generator::Counter => {
            let total = if count < 0 { 60 } else { count as usize };
            if count < 0 {
                log::warn!(
                    "Sample size not found for count=-1 and generator={:?}, defaulting to count=60",
                    s.generator
                );
            }
            let mut start = 0;
            while start < total {
                let c = (total - start).min(CHUNK_EVENTS);
                out.push(match s.generator {
                    Generator::Windbag => Plan::Windbag { start, count: c, total },
                    _ => Plan::Counter { start, count: c, total },
                });
                start += c;
            }
        }
        _ => {
            if n == 0 {
                return out;
            }
            if count < 1 && count != -1 {
                log::info!("There is no data to be generated because the count is {}.", count);
                return out;
            }
            if s.randomize_events {
                let total = if count == -1 { n } else { count as usize };
                let mut start = 0;
                while start < total {
                    let c = (total - start).min(CHUNK_EVENTS);
                    out.push(Plan::RandomLines { count: c });
                    start += c;
                }
            } else if s.bundlelines {
                let copies = if count == -1 { 1 } else { count as usize };
                let per_job = (CHUNK_EVENTS / n).max(1);
                let mut done = 0;
                while done < copies {
                    let c = (copies - done).min(per_job);
                    out.push(Plan::Bundle { copies: c });
                    done += c;
                }
            } else {
                let total = if count == -1 { n } else { count as usize };
                let mut start = 0;
                while start < total {
                    let c = (total - start).min(CHUNK_EVENTS);
                    out.push(Plan::Lines { start: start % n, count: c });
                    start += c;
                }
            }
        }
    }
    out
}

/// Plans for a byte-driven interval (the perdayvolume generator).
pub fn plan_bytes(s: &Sample, size: f64, _rng: &mut SmallRng) -> Vec<Plan> {
    let n = s.data.len();
    let mut out = Vec::new();
    if n == 0 || size <= 0.0 {
        return out;
    }
    let size = size * s.pdv_ratio;
    if s.randomize_events {
        let mut remaining = size as u64;
        while remaining > 0 {
            let c = remaining.min(CHUNK_BYTES);
            out.push(Plan::RandomBytes { size: c });
            remaining -= c;
        }
    } else if s.bundlelines {
        let total = s.data.total_size().max(1) as f64;
        let copies = (size / total).floor() as usize + 1;
        let per_job = (CHUNK_EVENTS / n).max(1);
        let mut done = 0;
        while done < copies {
            let c = (copies - done).min(per_job);
            out.push(Plan::Bundle { copies: c });
            done += c;
        }
    } else {
        let first = s.data.lines[0].size as f64;
        if size < first {
            log::error!(
                "Size is too small for sample {}. We need {} bytes but size of one event is {} bytes.",
                s.name,
                size,
                first
            );
            return out;
        }
        let total = s.data.total_size().max(1);
        let cycles = (size as u64) / total;
        let rem = (size as u64) - cycles * total;
        // largest k with prefix[k] <= rem
        let k = match s.data.prefix.binary_search(&rem) {
            Ok(i) => i,
            Err(i) => i - 1,
        };
        let events = cycles as usize * n + k;
        let mut start = 0;
        while start < events {
            let c = (events - start).min(CHUNK_EVENTS);
            out.push(Plan::Lines { start: start % n, count: c });
            start += c;
        }
    }
    out
}

/// The perdayvolume generator sizes its fill by the ratio of the sample's
/// pre- and post-token-replacement byte sizes. Upstream recomputes this every
/// interval with fresh random values; once at load is statistically the same.
pub fn perdayvolume_ratio(s: &Sample, seed: Option<u64>) -> f64 {
    use rand::SeedableRng;
    let mut rng = match seed {
        Some(v) => SmallRng::seed_from_u64(v ^ 0x5eed),
        None => SmallRng::from_os_rng(),
    };
    let now = s.now();
    let now_ts = s.ts(&now);
    let maps: &RateMaps = &s.rate_maps;
    let mut ctx = EventCtx::new(&mut rng, now_ts, maps);
    ctx.pivot = Some(now_ts);
    ctx.et = Some(now_ts);
    ctx.lt = Some(now_ts);
    let mut pre = 0u64;
    let mut post = 0u64;
    for line in &s.data.lines {
        ctx.begin_event();
        let mut raw = line.raw.clone();
        for t in &s.tokens {
            t.replace(&mut raw, &mut ctx);
        }
        pre += line.size as u64;
        post += raw.len() as u64 + 1;
    }
    if post == 0 {
        1.0
    } else {
        pre as f64 / post as f64
    }
}

/// `str(datetime)` for an epoch with a fraction: `YYYY-MM-DD HH:MM:SS[.ffffff]`.
pub fn py_datetime_str(epoch: f64) -> String {
    let secs = epoch.floor() as i64;
    let micro = ((epoch - epoch.floor()) * 1e6).round() as u32;
    let micro = micro.min(999_999);
    match Ts::from_epoch(secs, micro) {
        Some(ts) => {
            let base = format!(
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second
            );
            if micro > 0 {
                format!("{}.{:06}", base, micro)
            } else {
                base
            }
        }
        None => String::new(),
    }
}

/// Python's `str()` of an int-or-float counter value.
pub fn py_num_str(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        crate::token::py_float_str(v)
    }
}

#[allow(dead_code)]
fn _unused(r: &mut SmallRng) -> u8 {
    r.random()
}
