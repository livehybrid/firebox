//! The engine: resolved samples, per-sample timers (`lib/eventgentimer.py`)
//! and the generator worker pool.
//!
//! A timer fires every `interval` seconds, rates the sample (config or
//! perdayvolume rater) and turns the interval's work into small jobs that any
//! worker can run: "lines 512..1023 of sample X with window [et, lt]". Workers
//! template events, replace tokens and write batches to the sample's output.
//! Replay samples are engine-paced and run inside their own timer thread.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::NaiveDateTime;
use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use crate::clock;
use crate::conf::{parse_bool, Conf, StanzaConf};
use crate::envelope::{EventOut, Meta, TimeVal};
use crate::gen;
use crate::output::{Output, Outputs, Writer};
use crate::pyre::RxLocal;
use crate::rate::{randomize_count_factor, rated_count, PerDayVolumeRater, RateMaps};
use crate::sample::{self, SampleData, DEFAULT_BREAKER};
use crate::strftime::Ts;
use crate::timeparse;
use crate::token::{EventCtx, Token};

/// Events per job. Bounds per-batch memory and lets one interval spread over
/// every worker.
pub const CHUNK_EVENTS: usize = 512;
/// Bytes per job for byte-driven (perdayvolume + randomizeEvents) fills.
pub const CHUNK_BYTES: u64 = 128 * 1024;
const JOB_QUEUE: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Generator {
    Default,
    PerDayVolume,
    Replay,
    Windbag,
    Counter,
}

#[derive(Debug, Clone, PartialEq)]
pub enum End {
    /// `end = 0`: generate nothing.
    Nothing,
    /// `end = -1` or unset: run until stopped.
    Never,
    Executions(i64),
    At(NaiveDateTime),
}

#[derive(Debug, Clone, Copy)]
pub enum Tz {
    Local,
    /// `timezone = +HHMM`: naive `utcnow() + offset`.
    Offset(i64),
}

#[derive(Debug, Clone)]
pub struct CounterSettings {
    pub template: String,
    pub start: f64,
    pub end: f64,
    pub by: f64,
}

/// A replay-mode line with its parsed timestamp and the gap to its predecessor.
#[derive(Debug, Clone)]
pub struct ReplayLine {
    pub idx: usize,
    pub base_time: NaiveDateTime,
    pub diff: f64,
}

pub struct Sample {
    pub conf: StanzaConf,
    pub name: String,
    pub data: SampleData,
    pub tokens: Vec<Token>,
    pub host_token: Option<Token>,
    pub meta: Arc<Meta>,
    pub index_list: Vec<Arc<str>>,
    pub rate_maps: RateMaps,
    pub randomize_count: Option<f64>,
    pub randomize_events: bool,
    pub bundlelines: bool,
    pub sequential_timestamp: bool,
    pub interval: i64,
    pub delay: f64,
    pub count: i64,
    pub earliest: String,
    pub latest: String,
    pub tz: Tz,
    pub generator: Generator,
    pub per_day_volume: Option<f64>,
    /// pre/post token-replacement size ratio for the perdayvolume fill.
    pub pdv_ratio: f64,
    pub time_multiple: f64,
    pub end: End,
    pub backfill: Option<String>,
    pub batch: usize,
    pub output: Arc<dyn Output>,
    pub counter: Option<CounterSettings>,
    pub replay_lines: Vec<ReplayLine>,
}

impl Sample {
    pub fn build(conf: StanzaConf, output: Arc<dyn Output>, seed: Option<u64>) -> anyhow::Result<Sample> {
        let cwd = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
        let generator = match conf.generator() {
            "default" => Generator::Default,
            "perdayvolumegenerator" => Generator::PerDayVolume,
            "replay" => Generator::Replay,
            "windbag" => Generator::Windbag,
            "counter" => Generator::Counter,
            other => anyhow::bail!("generator '{}' is not supported by firebox (stanza '{}')", other, conf.name),
        };
        let opt = |k: &str| conf.get(k).filter(|v| !v.trim().is_empty()).map(Arc::<str>::from);
        let meta = Arc::new(Meta {
            index: opt("index"),
            host: opt("host"),
            source: opt("source"),
            sourcetype: opt("sourcetype"),
        });
        let breaker = conf.get("breaker").unwrap_or(DEFAULT_BREAKER).to_string();
        let data = match (&conf.file_path, conf.get("sampletype").unwrap_or("raw")) {
            (Some(p), "csv") => sample::load_csv(p, &meta, conf.get("hostRegex"))?,
            (Some(p), _) => sample::load_raw(p, &breaker)?,
            (None, _) => SampleData::default(),
        };
        if matches!(generator, Generator::Default | Generator::PerDayVolume | Generator::Replay) && data.is_empty() {
            anyhow::bail!("sample '{}' has no events", conf.name);
        }
        let mut tokens = Vec::with_capacity(conf.tokens.len());
        for spec in conf.tokens.iter() {
            let t = Token::compile(spec, &cwd, &conf.sample_dir, 0)
                .map_err(|e| anyhow::anyhow!("stanza '{}' token {}: {}", conf.name, spec.index, e))?;
            tokens.push(t);
        }
        // Multivalue file tokens share one picked line per event when they
        // read the same file (upstream keys its mvhash by file path), so the
        // cache id is per distinct path, not per token.
        let mut paths: Vec<std::path::PathBuf> = Vec::new();
        for t in tokens.iter_mut() {
            if let crate::token::Kind::File(f) = &mut t.kind {
                let id = match paths.iter().position(|p| *p == f.path) {
                    Some(i) => i + 1,
                    None => {
                        paths.push(f.path.clone());
                        paths.len()
                    }
                };
                f.file_id = id;
            }
        }
        let host_token = match &conf.host_token {
            Some((tok, rep)) => {
                let spec = crate::conf::TokenSpec {
                    index: 0,
                    token: tok.clone(),
                    replacement_type: "file".into(),
                    replacement: rep.clone(),
                };
                Some(
                    Token::compile(&spec, &cwd, &conf.sample_dir, 0)
                        .map_err(|e| anyhow::anyhow!("stanza '{}' host token: {}", conf.name, e))?,
                )
            }
            None => None,
        };
        let tz = match conf.get("timezone").map(str::trim) {
            None | Some("") | Some("local") => Tz::Local,
            Some(v) => match v.parse::<i64>() {
                Ok(n) => {
                    let hours = n / 100;
                    let minutes = n % 100;
                    Tz::Offset(hours * 3600 + minutes * 60)
                }
                Err(_) => {
                    log::error!("Could not parse timezone {}", v);
                    Tz::Local
                }
            },
        };
        let end = match conf.get("end").map(str::trim).filter(|v| !v.is_empty()) {
            None => End::Never,
            Some(v) => match v.parse::<i64>() {
                Ok(0) => End::Nothing,
                Ok(n) if n < 0 => End::Never,
                Ok(n) => End::Executions(n),
                Err(_) => match timeparse::parse_time(v, clock::now_local()) {
                    Ok(t) => End::At(t),
                    Err(e) => anyhow::bail!("Failed to parse end '{}' for sample '{}': {}", v, conf.name, e),
                },
            },
        };
        let counter = if generator == Generator::Counter {
            Some(CounterSettings {
                template: conf.get("count_template").unwrap_or(
                    "{event_ts}-0700 Counter for sample:{samplename}, Now processing event counting {loop_count} of {max_loop} cycles. Counter Values: Start_Count: {start_count} Current_Counter:{current_count} End_Count:{end_count} Counting_By: {count_by}",
                ).to_string(),
                start: conf.get_f64("start_count").unwrap_or(0.0),
                end: conf.get_f64("end_count").unwrap_or(0.0),
                by: conf.get_f64("count_by").unwrap_or(1.0),
            })
        } else {
            None
        };
        let batch = conf.get_i64("maxQueueLength").filter(|v| *v > 1).map(|v| v as usize).unwrap_or(CHUNK_EVENTS);
        let mut s = Sample {
            name: conf.name.clone(),
            index_list: conf
                .get("extendIndexes")
                .map(sample::parse_extend_indexes)
                .unwrap_or_default()
                .into_iter()
                .map(Arc::from)
                .collect(),
            rate_maps: RateMaps::parse(|k| conf.get(k).map(str::to_string)),
            randomize_count: conf.get_f64("randomizeCount").filter(|v| *v > 0.0),
            randomize_events: conf.get_bool("randomizeEvents").unwrap_or(false),
            bundlelines: conf.get_bool("bundlelines").unwrap_or(false),
            sequential_timestamp: conf.get_bool("sequentialTimestamp").unwrap_or(false),
            interval: conf.get_i64("interval").unwrap_or(60),
            delay: conf.get_f64("delay").unwrap_or(0.0),
            count: conf.get_i64("count").unwrap_or(-1),
            earliest: conf.get_str("earliest", "now"),
            latest: conf.get_str("latest", "now"),
            tz,
            generator,
            per_day_volume: conf.get_f64("perDayVolume").filter(|v| *v > 0.0),
            pdv_ratio: 1.0,
            time_multiple: conf.get_f64("timeMultiple").unwrap_or(1.0),
            end,
            backfill: conf.get("backfill").map(str::trim).filter(|v| !v.is_empty()).map(str::to_string),
            batch,
            output,
            counter,
            replay_lines: Vec::new(),
            data,
            tokens,
            host_token,
            meta,
            conf,
        };
        let _ = parse_bool;
        if s.generator == Generator::PerDayVolume {
            s.pdv_ratio = gen::perdayvolume_ratio(&s, seed);
        }
        if s.generator == Generator::Replay {
            s.replay_lines = gen::replay::build_replay_lines(&s);
            if s.replay_lines.is_empty() {
                anyhow::bail!("replay sample '{}' has no events with a parseable timestamp", s.name);
            }
        }
        Ok(s)
    }

    /// `Sample.now()`: naive local time, or `utcnow() + offset` with a `timezone`.
    pub fn now(&self) -> NaiveDateTime {
        match self.tz {
            Tz::Local => clock::now_local(),
            Tz::Offset(secs) => {
                chrono::Utc::now().naive_utc()
                    + Duration::from_secs(secs.unsigned_abs()) * if secs >= 0 { 1 } else { 0 }
                    - Duration::from_secs(secs.unsigned_abs()) * if secs < 0 { 1 } else { 0 }
            }
        }
    }

    pub fn ts(&self, dt: &NaiveDateTime) -> Ts {
        Ts::from_naive(dt, clock::epoch_from_local(dt))
    }

    /// The generation window for a fire at `now`.
    pub fn window(&self, now: NaiveDateTime) -> anyhow::Result<(NaiveDateTime, NaiveDateTime)> {
        let et = timeparse::parse_time(&self.earliest, now)
            .map_err(|e| anyhow::anyhow!("earliest '{}': {}", self.earliest, e))?;
        let lt =
            timeparse::parse_time(&self.latest, now).map_err(|e| anyhow::anyhow!("latest '{}': {}", self.latest, e))?;
        Ok((et, lt))
    }

    pub fn is_pooled(&self) -> bool {
        self.generator != Generator::Replay
    }

    pub fn save_state(&self) {
        for t in &self.tokens {
            t.save_state(&self.conf.sample_dir);
        }
    }
}

#[derive(Debug, Clone)]
pub enum Plan {
    /// Sequential lines from `start` (mod the sample length).
    Lines {
        start: usize,
        count: usize,
    },
    RandomLines {
        count: usize,
    },
    /// Whole-file copies (`bundlelines`).
    Bundle {
        copies: usize,
    },
    /// Random lines until `size` bytes (perdayvolume + randomizeEvents).
    RandomBytes {
        size: u64,
    },
    Windbag {
        start: usize,
        count: usize,
        total: usize,
    },
    Counter {
        start: usize,
        count: usize,
        total: usize,
    },
}

pub struct Job {
    pub sample: Arc<Sample>,
    pub plan: Plan,
    pub et: Ts,
    pub lt: Ts,
    /// (offset, total) for `sequentialTimestamp` numbering across chunks.
    pub seq: Option<(usize, usize)>,
    /// Drop the job unrun after this instant (a stalled sink must not replay
    /// the past). `None` for backfill work.
    pub deadline: Option<Instant>,
    pub now: Ts,
}

#[derive(Default)]
pub struct Stats {
    pub events: AtomicU64,
    pub bytes: AtomicU64,
    pub intervals: AtomicU64,
    pub skipped_intervals: AtomicU64,
    pub stale_jobs: AtomicU64,
    pub errors: AtomicU64,
}

impl Stats {
    pub fn add(&self, events: u64, bytes: u64) {
        self.events.fetch_add(events, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
    }
}

pub struct EngineOptions {
    pub threads: usize,
    pub seed: Option<u64>,
    pub duration: Option<Duration>,
    pub stats_every: Option<Duration>,
}

pub struct Engine {
    samples: Vec<Arc<Sample>>,
    outputs: Outputs,
    opts: EngineOptions,
    stop: Arc<AtomicBool>,
    stats: Arc<Stats>,
    failed: Arc<AtomicBool>,
}

impl Engine {
    pub fn new(conf: Conf, outputs: Outputs, opts: EngineOptions, stop: Arc<AtomicBool>) -> anyhow::Result<Engine> {
        let mut samples = Vec::new();
        for sc in conf.samples {
            let output = outputs.get(&sc);
            let s = Sample::build(sc, output, opts.seed)?;
            log::info!(
                "sample '{}': generator={:?} interval={} count={} tokens={} events={} output={}",
                s.name,
                s.generator,
                s.interval,
                s.count,
                s.tokens.len(),
                s.data.len(),
                s.output.name()
            );
            samples.push(Arc::new(s));
        }
        Ok(Engine {
            samples,
            outputs,
            opts,
            stop,
            stats: Arc::new(Stats::default()),
            failed: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn samples(&self) -> &[Arc<Sample>] {
        &self.samples
    }

    pub fn stats(&self) -> Arc<Stats> {
        self.stats.clone()
    }

    /// Run until every bounded sample finishes or the stop flag is raised.
    /// Returns an error when a worker hit a fatal sink failure.
    pub fn run(self) -> anyhow::Result<()> {
        if self.samples.is_empty() {
            log::info!("No samples found. Exiting.");
            return Ok(());
        }
        let (tx, rx) = bounded::<Job>(JOB_QUEUE);
        let threads = self.opts.threads.max(1);
        let mut workers = Vec::with_capacity(threads);
        for i in 0..threads {
            let rx = rx.clone();
            let stop = self.stop.clone();
            let stats = self.stats.clone();
            let failed = self.failed.clone();
            let seed = self.opts.seed;
            workers.push(std::thread::Builder::new().name(format!("firebox-gen-{}", i)).spawn(move || {
                worker_loop(i, rx, stop, stats, failed, seed);
            })?);
        }
        drop(rx);

        let started = Instant::now();
        let mut timers = Vec::new();
        for (i, s) in self.samples.iter().enumerate() {
            let s = s.clone();
            let tx = tx.clone();
            let stop = self.stop.clone();
            let stats = self.stats.clone();
            let failed = self.failed.clone();
            let seed = self.opts.seed.map(|v| v ^ (0xA5A5_0000 + i as u64));
            timers.push(std::thread::Builder::new().name(format!("firebox-timer-{}", s.name)).spawn(move || {
                if let Err(e) = timer_loop(s, tx, stop.clone(), stats, seed) {
                    log::error!("{}", e);
                    failed.store(true, Ordering::Relaxed);
                    stop.store(true, Ordering::Relaxed);
                }
            })?);
        }
        drop(tx);

        let stats_thread = self.opts.stats_every.map(|every| {
            let stats = self.stats.clone();
            let stop = self.stop.clone();
            std::thread::spawn(move || stats_loop(stats, stop, every))
        });

        // Wait for the timers; a duration bound raises the stop flag.
        loop {
            let all_done = timers.iter().all(|t| t.is_finished());
            if all_done {
                break;
            }
            if let Some(d) = self.opts.duration {
                if started.elapsed() >= d {
                    log::info!("duration reached; stopping");
                    self.stop.store(true, Ordering::Relaxed);
                }
            }
            if self.failed.load(Ordering::Relaxed) {
                self.stop.store(true, Ordering::Relaxed);
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        for t in timers {
            let _ = t.join();
        }
        // Timers are gone, so the channel closes once the last sender drops
        // (they held the clones); workers finish the queue and exit.
        for w in workers {
            let _ = w.join();
        }
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = stats_thread {
            let _ = t.join();
        }
        self.outputs.finish_all()?;
        for s in &self.samples {
            s.save_state();
        }
        let ev = self.stats.events.load(Ordering::Relaxed);
        let by = self.stats.bytes.load(Ordering::Relaxed);
        let secs = started.elapsed().as_secs_f64().max(1e-9);
        log::info!(
            "done: {} events, {:.1} MB in {:.1}s ({:.0} eps, {:.2} MB/s); intervals={} skipped={} stale_jobs={}",
            ev,
            by as f64 / 1e6,
            secs,
            ev as f64 / secs,
            by as f64 / 1e6 / secs,
            self.stats.intervals.load(Ordering::Relaxed),
            self.stats.skipped_intervals.load(Ordering::Relaxed),
            self.stats.stale_jobs.load(Ordering::Relaxed)
        );
        if self.failed.load(Ordering::Relaxed) {
            anyhow::bail!("generation aborted after a fatal output error");
        }
        Ok(())
    }
}

fn stats_loop(stats: Arc<Stats>, stop: Arc<AtomicBool>, every: Duration) {
    let mut last_e = 0u64;
    let mut last_b = 0u64;
    let mut last_t = Instant::now();
    while !stop.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(100));
        if last_t.elapsed() < every {
            continue;
        }
        let e = stats.events.load(Ordering::Relaxed);
        let b = stats.bytes.load(Ordering::Relaxed);
        let dt = last_t.elapsed().as_secs_f64();
        log::info!(
            "stats: {:.0} eps, {:.2} MB/s (total {} events)",
            (e - last_e) as f64 / dt,
            (b - last_b) as f64 / 1e6 / dt,
            e
        );
        last_e = e;
        last_b = b;
        last_t = Instant::now();
    }
}

fn make_rng(seed: Option<u64>, salt: u64) -> SmallRng {
    match seed {
        Some(s) => SmallRng::seed_from_u64(s.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(salt)),
        None => SmallRng::from_os_rng(),
    }
}

/// Interruptible sleep until `deadline`.
fn sleep_until(deadline: Instant, stop: &AtomicBool) {
    loop {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        let now = Instant::now();
        if now >= deadline {
            return;
        }
        std::thread::sleep((deadline - now).min(Duration::from_millis(100)));
    }
}

fn timer_loop(
    s: Arc<Sample>,
    tx: Sender<Job>,
    stop: Arc<AtomicBool>,
    stats: Arc<Stats>,
    seed: Option<u64>,
) -> anyhow::Result<()> {
    let mut rng = make_rng(seed, 7);
    if s.delay > 0.0 {
        log::info!("Sample set to delay {}, sleeping.", s.delay);
        sleep_until(Instant::now() + Duration::from_secs_f64(s.delay), &stop);
    }
    match s.end {
        End::Nothing => {
            log::info!("End = 0, no events will be generated for sample '{}'", s.name);
            return Ok(());
        }
        End::Never => log::info!("End is set to -1. Will be running without stopping for sample {}", s.name),
        _ => {}
    }
    let interval = Duration::from_secs(if s.interval > 0 { s.interval as u64 } else { 1 });
    let mut pdv = s.per_day_volume.map(|gb| {
        let raw_event_size = if s.data.is_empty() { 0.0 } else { s.data.total_size() as f64 / s.data.len() as f64 };
        PerDayVolumeRater::new(gb, s.interval, raw_event_size)
    });
    let mut replay_writer: Option<Box<dyn Writer>> = None;
    let mut executions: i64 = 0;
    let mut backfill_done = false;
    let mut next_fire = Instant::now();
    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let now = s.now();
        if let End::At(t) = s.end {
            if now >= t {
                log::info!("End Time '{}' reached, ending generation of sample '{}'", t, s.name);
                break;
            }
        }
        let fire = Instant::now();
        let now_ts = s.ts(&now);
        let factor = s.randomize_count.map(|rc| randomize_count_factor(rc, &mut rng)).unwrap_or(1.0)
            * s.rate_maps.factor(&now_ts);

        if s.generator == Generator::Replay {
            let w = match replay_writer.as_mut() {
                Some(w) => w,
                None => replay_writer.insert(s.output.open_writer(usize::MAX)?),
            };
            gen::replay::run_replay(&s, w.as_mut(), &mut rng, &stop, &stats, &mut backfill_done)?;
            w.flush()?;
        } else if s.backfill.is_some() && !backfill_done {
            backfill_sweep(&s, &tx, &mut rng, &mut pdv, now, &stats)?;
            backfill_done = true;
        } else {
            let (et, lt) = s.window(now)?;
            let deadline = Some(fire + interval.max(Duration::from_secs(1)));
            let jobs: Vec<Plan> = match s.generator {
                Generator::PerDayVolume => match pdv.as_mut().and_then(|r| r.next_size(factor)) {
                    Some(size) => gen::plan_bytes(&s, size, &mut rng),
                    None => Vec::new(),
                },
                _ => {
                    let base =
                        if s.count == -1 && s.generator == Generator::Default { s.data.len() as i64 } else { s.count };
                    let count = rated_count(base, factor);
                    gen::plan_count(&s, count)
                }
            };
            let total: usize = jobs.iter().map(gen::plan_events).sum();
            let mut offset = 0usize;
            let mut sent_all = true;
            for plan in jobs {
                let n = gen::plan_events(&plan);
                let job = Job {
                    sample: s.clone(),
                    plan,
                    et: s.ts(&et),
                    lt: s.ts(&lt),
                    seq: Some((offset, total)),
                    deadline,
                    now: now_ts,
                };
                offset += n;
                match tx.try_send(job) {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) => {
                        log::warn!("Generator queue full. Skipping current generation.");
                        stats.skipped_intervals.fetch_add(1, Ordering::Relaxed);
                        sent_all = false;
                        break;
                    }
                    Err(TrySendError::Disconnected(_)) => return Ok(()),
                }
            }
            if sent_all {
                stats.intervals.fetch_add(1, Ordering::Relaxed);
            }
        }
        executions += 1;
        if let End::Executions(n) = s.end {
            if executions >= n {
                log::info!("End executions {} reached, ending generation of sample '{}'", n, s.name);
                break;
            }
        }
        next_fire += interval;
        let now_i = Instant::now();
        if next_fire < now_i {
            // late by more than an interval: do not burst to catch up
            next_fire = now_i;
        }
        sleep_until(next_fire, &stop);
    }
    if let Some(w) = replay_writer.as_mut() {
        w.flush()?;
    }
    Ok(())
}

/// `backfill = -1h`: generate every interval from then until now, each with its
/// own window and (rate-map) time, then carry on live.
fn backfill_sweep(
    s: &Arc<Sample>,
    tx: &Sender<Job>,
    rng: &mut SmallRng,
    pdv: &mut Option<PerDayVolumeRater>,
    now: NaiveDateTime,
    stats: &Stats,
) -> anyhow::Result<()> {
    let spec = s.backfill.as_deref().unwrap_or("now");
    let mut t = timeparse::parse_time(spec, now).map_err(|e| anyhow::anyhow!("backfill '{}': {}", spec, e))?;
    let step = chrono::Duration::seconds(s.interval.max(1));
    let mut intervals = 0u64;
    while t < now {
        let et = t;
        let lt = t + step;
        let ts = s.ts(&t);
        let factor =
            s.randomize_count.map(|rc| randomize_count_factor(rc, rng)).unwrap_or(1.0) * s.rate_maps.factor(&ts);
        let plans: Vec<Plan> = match s.generator {
            Generator::PerDayVolume => match pdv.as_mut().and_then(|r| r.next_size(factor)) {
                Some(size) => gen::plan_bytes(s, size, rng),
                None => Vec::new(),
            },
            _ => {
                let base =
                    if s.count == -1 && s.generator == Generator::Default { s.data.len() as i64 } else { s.count };
                gen::plan_count(s, rated_count(base, factor))
            }
        };
        let total: usize = plans.iter().map(gen::plan_events).sum();
        let mut offset = 0usize;
        for plan in plans {
            let n = gen::plan_events(&plan);
            let job = Job {
                sample: s.clone(),
                plan,
                et: s.ts(&et),
                lt: s.ts(&lt),
                seq: Some((offset, total)),
                deadline: None,
                now: ts,
            };
            offset += n;
            if tx.send(job).is_err() {
                return Ok(());
            }
        }
        intervals += 1;
        t = lt;
    }
    log::info!("backfill for sample '{}': queued {} interval(s) back to {}", s.name, intervals, spec);
    stats.intervals.fetch_add(intervals, Ordering::Relaxed);
    Ok(())
}

struct WorkerState {
    id: usize,
    rng: SmallRng,
    writers: Vec<(usize, Box<dyn Writer>)>,
    events: Vec<EventOut>,
    pool: Vec<String>,
    /// Direct-mapped cache of local broken-down time per epoch second.
    ts_cache: Box<[Option<(i64, Ts)>; 256]>,
    /// Per-sample regex handles owned by this thread: (sample key, tokens, host token).
    rx_local: Vec<(usize, Vec<RxLocal>, Option<RxLocal>)>,
}

impl WorkerState {
    /// Index of this thread's regex handles for `sample`, creating them on
    /// first use.
    fn local_rx_index(&mut self, sample: &Arc<Sample>) -> usize {
        let key = Arc::as_ptr(sample) as *const () as usize;
        if let Some(i) = self.rx_local.iter().position(|(k, _, _)| *k == key) {
            return i;
        }
        let toks = sample.tokens.iter().map(|t| RxLocal::new(&t.rx)).collect();
        let host = sample.host_token.as_ref().map(|t| RxLocal::new(&t.rx));
        self.rx_local.push((key, toks, host));
        self.rx_local.len() - 1
    }

    fn writer_for(&mut self, output: &Arc<dyn Output>) -> anyhow::Result<&mut dyn Writer> {
        let key = Arc::as_ptr(output) as *const () as usize;
        if let Some(pos) = self.writers.iter().position(|(k, _)| *k == key) {
            return Ok(self.writers[pos].1.as_mut());
        }
        let w = output.open_writer(self.id)?;
        self.writers.push((key, w));
        Ok(self.writers.last_mut().unwrap().1.as_mut())
    }
}

fn worker_loop(
    id: usize,
    rx: Receiver<Job>,
    stop: Arc<AtomicBool>,
    stats: Arc<Stats>,
    failed: Arc<AtomicBool>,
    seed: Option<u64>,
) {
    let mut state = WorkerState {
        id,
        rng: make_rng(seed, 1000 + id as u64),
        writers: Vec::new(),
        events: Vec::with_capacity(CHUNK_EVENTS),
        pool: Vec::with_capacity(CHUNK_EVENTS),
        ts_cache: Box::new([None; 256]),
        rx_local: Vec::new(),
    };
    for job in rx.iter() {
        if stop.load(Ordering::Relaxed) {
            continue; // drain without generating
        }
        if let Some(d) = job.deadline {
            if Instant::now() > d {
                stats.stale_jobs.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        }
        if let Err(e) = run_job(&job, &mut state, &stats) {
            log::error!("worker {}: {}", id, e);
            stats.errors.fetch_add(1, Ordering::Relaxed);
            failed.store(true, Ordering::Relaxed);
            stop.store(true, Ordering::Relaxed);
        }
    }
    for (_, w) in state.writers.iter_mut() {
        if let Err(e) = w.flush() {
            log::error!("worker {}: flush failed: {}", id, e);
        }
    }
}

fn run_job(job: &Job, st: &mut WorkerState, stats: &Stats) -> anyhow::Result<()> {
    let s = &*job.sample;
    let batch_index: Option<Arc<str>> = if s.index_list.is_empty() {
        None
    } else {
        Some(s.index_list[st.rng.random_range(0..s.index_list.len())].clone())
    };
    let batch_meta: Arc<Meta> = match &batch_index {
        Some(idx) => Arc::new(Meta {
            index: Some(idx.clone()),
            host: s.meta.host.clone(),
            source: s.meta.source.clone(),
            sourcetype: s.meta.sourcetype.clone(),
        }),
        None => s.meta.clone(),
    };
    st.events.clear();
    let (et, lt) = (job.et, job.lt);
    let mut bytes = 0u64;
    let li = st.local_rx_index(&job.sample);
    {
        let WorkerState { rng, events, pool, ts_cache, rx_local, .. } = st;
        let (_, tok_rx, host_rx) = &mut rx_local[li];
        let mut ctx = EventCtx::new(rng, job.now, &s.rate_maps);
        ctx.et = Some(et);
        ctx.lt = Some(lt);
        let cache: &mut [Option<(i64, Ts)>; 256] = ts_cache;
        let mut emit = |line: &sample::Line, seq_i: Option<usize>, ctx: &mut EventCtx<'_>| {
            ctx.begin_event();
            let pivot_epoch: i64;
            let pivot = match (s.sequential_timestamp && s.generator != Generator::PerDayVolume, seq_i, job.seq) {
                (true, Some(i), Some((_, total))) if total > 0 => {
                    let span = (lt.epoch - et.epoch) as f64;
                    let f = et.epoch as f64 + span * i as f64 / total as f64;
                    pivot_epoch = f.floor() as i64;
                    let micro = ((f - f.floor()) * 1e6) as u32;
                    Ts::from_epoch(pivot_epoch, micro).unwrap_or(et)
                }
                _ => {
                    pivot_epoch =
                        if lt.epoch > et.epoch { ctx.rng.random_range(et.epoch..=lt.epoch) } else { et.epoch };
                    let slot = &mut cache[(pivot_epoch & 255) as usize];
                    match slot {
                        Some((e, ts)) if *e == pivot_epoch => *ts,
                        _ => {
                            let ts = Ts::from_epoch(pivot_epoch, 0).unwrap_or(et);
                            *slot = Some((pivot_epoch, ts));
                            ts
                        }
                    }
                }
            };
            ctx.pivot = Some(pivot);
            let mut raw = pool.pop().unwrap_or_default();
            raw.clear();
            raw.push_str(&line.raw);
            for (t, l) in s.tokens.iter().zip(tok_rx.iter_mut()) {
                t.replace_local(&mut raw, ctx, l);
            }
            let mut meta = match &line.meta {
                Some(m) => match &batch_index {
                    Some(idx) => Arc::new(Meta {
                        index: Some(idx.clone()),
                        host: m.host.clone(),
                        source: m.source.clone(),
                        sourcetype: m.sourcetype.clone(),
                    }),
                    None => m.clone(),
                },
                None => batch_meta.clone(),
            };
            if let (Some(ht), Some(hl)) = (&s.host_token, host_rx.as_mut()) {
                let mut h: String = meta.host.as_deref().unwrap_or("").to_string();
                ht.replace_local(&mut h, ctx, hl);
                meta = Arc::new(Meta {
                    index: meta.index.clone(),
                    host: Some(Arc::from(h)),
                    source: meta.source.clone(),
                    sourcetype: meta.sourcetype.clone(),
                });
            }
            bytes += raw.len() as u64;
            events.push(EventOut { raw, time: TimeVal::Int(pivot_epoch), meta });
        };
        let n = s.data.len();
        match &job.plan {
            Plan::Lines { start, count } => {
                for i in 0..*count {
                    let line = &s.data.lines[(start + i) % n];
                    let seq_i = job.seq.map(|(off, _)| off + i);
                    emit(line, seq_i, &mut ctx);
                }
            }
            Plan::RandomLines { count } => {
                for i in 0..*count {
                    let idx = ctx.rng.random_range(0..n);
                    let line = &s.data.lines[idx];
                    let seq_i = job.seq.map(|(off, _)| off + i);
                    emit(line, seq_i, &mut ctx);
                }
            }
            Plan::Bundle { copies } => {
                let mut i = 0usize;
                for _ in 0..*copies {
                    for line in &s.data.lines {
                        let seq_i = job.seq.map(|(off, _)| off + i);
                        emit(line, seq_i, &mut ctx);
                        i += 1;
                    }
                }
            }
            Plan::RandomBytes { size } => {
                let mut cur = 0u64;
                let mut i = 0usize;
                while cur < *size {
                    let idx = ctx.rng.random_range(0..n);
                    let line = &s.data.lines[idx];
                    emit(line, job.seq.map(|(off, _)| off + i), &mut ctx);
                    cur += line.size as u64;
                    i += 1;
                }
            }
            Plan::Windbag { start, count, total } => {
                let span = (lt.epoch - et.epoch) as f64 + (lt.micro as f64 - et.micro as f64) / 1e6;
                let step = span / (*total).max(1) as f64;
                let now_epoch = job.now.epoch;
                for i in 0..*count {
                    let idx = start + i;
                    let t = et.epoch as f64 + et.micro as f64 / 1e6 + step * (idx + 1) as f64;
                    let mut raw = pool.pop().unwrap_or_default();
                    raw.clear();
                    raw.push_str(&format!("{} -0700 WINDBAG Event {} of {}", gen::py_datetime_str(t), idx + 1, total));
                    bytes += raw.len() as u64;
                    events.push(EventOut { raw, time: TimeVal::Int(now_epoch), meta: batch_meta.clone() });
                }
            }
            Plan::Counter { start, count, total } => {
                if let Some(c) = &s.counter {
                    let span = (lt.epoch - et.epoch) as f64;
                    let step = span / (*total).max(1) as f64;
                    let now_epoch = job.now.epoch;
                    for i in 0..*count {
                        let idx = start + i;
                        let t = et.epoch as f64 + step * (idx + 1) as f64;
                        let current = c.start + c.by * idx as f64;
                        let text = c
                            .template
                            .replace("{event_ts}", &gen::py_datetime_str(t))
                            .replace("{samplename}", &s.name)
                            .replace("{loop_count}", &(idx + 1).to_string())
                            .replace("{max_loop}", &total.to_string())
                            .replace("{start_count}", &gen::py_num_str(c.start))
                            .replace("{current_count}", &gen::py_num_str(current))
                            .replace("{end_count}", &gen::py_num_str(c.end))
                            .replace("{count_by}", &gen::py_num_str(c.by));
                        let mut raw = pool.pop().unwrap_or_default();
                        raw.clear();
                        raw.push_str(&text);
                        bytes += raw.len() as u64;
                        events.push(EventOut { raw, time: TimeVal::Int(now_epoch), meta: batch_meta.clone() });
                    }
                }
            }
        }
    }
    if st.events.is_empty() {
        return Ok(());
    }
    let count = st.events.len() as u64;
    let events = std::mem::take(&mut st.events);
    let res = {
        let w = st.writer_for(&s.output)?;
        let mut r = Ok(());
        for chunk in events.chunks(s.batch) {
            r = w.write_batch(chunk);
            if r.is_err() {
                break;
            }
        }
        r
    };
    // hand the Strings back to the pool and keep the Vec's capacity
    let mut events = events;
    for e in events.drain(..) {
        st.pool.push(e.raw);
    }
    st.events = events;
    res?;
    stats.add(count, bytes);
    Ok(())
}
