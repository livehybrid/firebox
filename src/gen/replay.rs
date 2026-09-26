//! `mode = replay` (`plugins/generator/replay.py`): play a recorded sample back
//! with its original inter-event gaps (times `timeMultiple`), stamping each
//! event with the wall clock as it goes out and rewriting timestamp tokens to
//! that time.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use chrono::NaiveDateTime;
use rand::rngs::SmallRng;

use crate::clock;
use crate::engine::{ReplayLine, Sample, Stats};
use crate::envelope::{EventOut, Meta, TimeVal};
use crate::output::Writer;
use crate::strftime::Ts;
use crate::strptime::strptime;
use crate::timeparse;
use crate::token::{EventCtx, Kind};

/// Extract the event's timestamp using the sample's timestamp tokens
/// (`Sample.getTSFromEvent`).
pub fn ts_from_event(s: &Sample, text: &str, now: &NaiveDateTime) -> Option<NaiveDateTime> {
    for t in s.tokens.iter().filter(|t| t.is_timestamp()) {
        let Some(span) = t.rx.first(text) else { continue };
        let (a, b) =
            if t.rx.groups() == 0 { (span.start, span.end) } else { span.group1.unwrap_or((span.start, span.end)) };
        let mut time_string = &text[a..b];
        let fmt = t.spec.replacement.as_str();
        if fmt == "%s" {
            let digits = time_string.trim();
            let Ok(v) = digits.parse::<f64>() else { continue };
            let secs = if digits.len() < 10 { v } else { v / 10f64.powi((digits.len() - 10) as i32) };
            if let Some(dt) = clock::local_from_epoch_nanos(secs.floor() as i64, ((secs - secs.floor()) * 1e9) as u32) {
                return Some(dt);
            }
            continue;
        }
        // upstream chops a trailing +HHMM offset before strptime
        if time_string.len() >= 5 && time_string.as_bytes()[time_string.len() - 5] == b'+' {
            time_string = &time_string[..time_string.len() - 5];
        }
        match strptime(time_string, fmt) {
            Some(mut dt) => {
                if chrono::Datelike::year(&dt) == 1900 {
                    dt = dt.with_year(chrono::Datelike::year(now)).unwrap_or(dt);
                }
                return Some(dt);
            }
            None => {
                log::warn!("Match found ('{}') but time parse failed. Timeformat '{}'", time_string, fmt);
            }
        }
    }
    None
}

use chrono::Datelike;

pub fn build_replay_lines(s: &Sample) -> Vec<ReplayLine> {
    let now = s.now();
    let mut lines: Vec<ReplayLine> = Vec::new();
    for (idx, line) in s.data.lines.iter().enumerate() {
        let text = match &line.time_field {
            Some(tf) if s.conf.get("timeField").map(|v| v != "_raw").unwrap_or(false) => tf.as_str(),
            _ => line.raw.as_str(),
        };
        match ts_from_event(s, text, &now)
            .or_else(|| line.time_field.as_deref().and_then(|tf| ts_from_event(s, tf, &now)))
        {
            Some(base_time) => lines.push(ReplayLine { idx, base_time, diff: 0.0 }),
            None => log::error!("Extracting timestamp from an event failed (sample '{}', line {}).", s.name, idx + 1),
        }
    }
    for i in 1..lines.len() {
        let d = (lines[i].base_time - lines[i - 1].base_time).num_microseconds().unwrap_or(0) as f64 / 1e6;
        lines[i].diff = d * s.time_multiple;
    }
    lines
}

fn ts_of(dt: &NaiveDateTime) -> Ts {
    Ts::from_naive(dt, clock::epoch_from_local(dt))
}

fn time_val(dt: &NaiveDateTime) -> TimeVal {
    let ts = ts_of(dt);
    TimeVal::Float(ts.epoch as f64 + ts.micro as f64 / 1e6)
}

fn render(s: &Sample, rl: &ReplayLine, event_time: &NaiveDateTime, ctx: &mut EventCtx<'_>) -> EventOut {
    let line = &s.data.lines[rl.idx];
    ctx.begin_event();
    let ts = ts_of(event_time);
    ctx.pivot = None;
    ctx.et = Some(ts);
    ctx.lt = Some(ts);
    let mut raw = line.raw.clone();
    for t in &s.tokens {
        t.replace(&mut raw, ctx);
    }
    let mut meta = line.meta.clone().unwrap_or_else(|| s.meta.clone());
    if let Some(ht) = &s.host_token {
        let mut h: String = meta.host.as_deref().unwrap_or("").to_string();
        ht.replace(&mut h, ctx);
        meta = std::sync::Arc::new(Meta {
            index: meta.index.clone(),
            host: Some(h.into()),
            source: meta.source.clone(),
            sourcetype: meta.sourcetype.clone(),
        });
    }
    EventOut { raw, time: time_val(event_time), meta }
}

/// Interruptible sleep for a replay gap.
fn sleep_gap(secs: f64, stop: &AtomicBool) -> bool {
    if secs <= 0.0 {
        return true;
    }
    let deadline = Instant::now() + Duration::from_secs_f64(secs);
    loop {
        if stop.load(Ordering::Relaxed) {
            return false;
        }
        let now = Instant::now();
        if now >= deadline {
            return true;
        }
        std::thread::sleep((deadline - now).min(Duration::from_millis(100)));
    }
}

/// One pass over the sample. Returns after the last event (or on stop).
pub fn run_replay(
    s: &Sample,
    writer: &mut dyn Writer,
    rng: &mut SmallRng,
    stop: &AtomicBool,
    stats: &Stats,
    backfill_done: &mut bool,
) -> anyhow::Result<()> {
    let lines = &s.replay_lines;
    if lines.is_empty() {
        return Ok(());
    }
    let current_time = s.now();
    let now_ts = ts_of(&current_time);
    let maps = &s.rate_maps;
    let mut ctx = EventCtx::new(rng, now_ts, maps);
    let mut batch: Vec<EventOut> = Vec::new();
    let flush = |batch: &mut Vec<EventOut>, writer: &mut dyn Writer| -> anyhow::Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let bytes: u64 = batch.iter().map(|e| e.raw.len() as u64).sum();
        writer.write_batch(batch)?;
        stats.add(batch.len() as u64, bytes);
        batch.clear();
        Ok(())
    };

    // Backfill: walk the recording backwards from now until the backfill
    // horizon, then emit those events oldest first.
    if let Some(spec) = &s.backfill {
        if !*backfill_done {
            match timeparse::parse_time(spec, current_time) {
                Ok(horizon) if horizon < current_time => {
                    let mut t = current_time;
                    let mut idx = lines.len() - 1;
                    let mut events = Vec::new();
                    while t >= horizon {
                        let rl = &lines[idx];
                        t -= chrono::Duration::microseconds((rl.diff * 1e6) as i64);
                        events.push(render(s, rl, &t, &mut ctx));
                        if idx == 0 {
                            idx = lines.len() - 1;
                        } else {
                            idx -= 1;
                        }
                        if events.len() > 5_000_000 {
                            log::warn!("replay backfill for '{}' capped at 5M events", s.name);
                            break;
                        }
                    }
                    events.reverse();
                    for chunk in events.chunks(1024) {
                        let bytes: u64 = chunk.iter().map(|e| e.raw.len() as u64).sum();
                        writer.write_batch(chunk)?;
                        stats.add(chunk.len() as u64, bytes);
                    }
                }
                Ok(_) => {}
                Err(e) => log::error!("Failed to parse backfill '{}': {}", spec, e),
            }
            *backfill_done = true;
        }
    }

    let mut prev_diff = 0.0;
    for (i, rl) in lines.iter().enumerate() {
        if i > 0 {
            if prev_diff > 0.0 {
                flush(&mut batch, writer)?;
                if !sleep_gap(prev_diff, stop) {
                    flush(&mut batch, writer)?;
                    return Ok(());
                }
            }
            if prev_diff < 0.0 {
                log::error!(
                    "Can't sleep for negative time, please make sure your events are in time order. see line Number{}",
                    i
                );
            }
        }
        let event_time = s.now();
        batch.push(render(s, rl, &event_time, &mut ctx));
        if batch.len() >= 512 {
            flush(&mut batch, writer)?;
        }
        prev_diff = rl.diff;
        if stop.load(Ordering::Relaxed) {
            break;
        }
    }
    flush(&mut batch, writer)?;
    Ok(())
}

#[allow(dead_code)]
fn _kind_used(k: &Kind) -> bool {
    matches!(k, Kind::Broken)
}
