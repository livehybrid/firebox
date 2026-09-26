use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Args, Parser, Subcommand, ValueEnum};

use firebox::conf::{self, LoadOptions};
use firebox::engine::{Engine, EngineOptions};
use firebox::output::{OutputOptions, Outputs, SocketConnections};

/// A fast, multi-threaded, eventgen.conf-compatible event generator.
#[derive(Parser, Debug)]
#[command(name = "firebox", version, about, long_about = None)]
struct Cli {
    /// Increase log verbosity (-v info, -vv debug, -vvv trace). Default: error.
    #[arg(short = 'v', long = "verbosity", action = clap::ArgAction::Count, global = true)]
    verbosity: u8,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Generate events using a supplied config file (eventgen `generate`).
    Generate(GenerateArgs),
    /// Parse and resolve a config, then print what would run.
    Validate(ValidateArgs),
    /// Measure raw generation throughput (no timers, output discarded).
    Bench(BenchArgs),
    /// Print the version.
    Version,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Connections {
    PerThread,
    Single,
}

#[derive(Args, Debug)]
struct GenerateArgs {
    /// Location of eventgen.conf or an app folder holding default/eventgen.conf.
    configfile: PathBuf,
    /// Run the specified sample only, disabling all other samples.
    #[arg(short = 's', long = "sample")]
    sample: Option<String>,
    /// Keep the original outputMode of the sample (the default).
    #[arg(long)]
    keepoutput: bool,
    /// Set outputMode to devnull.
    #[arg(long)]
    devnull: bool,
    /// Set outputMode to stdout.
    #[arg(long)]
    stdout: bool,
    /// Set outputMode to modinput, to see metadata.
    #[arg(long)]
    modinput: bool,
    /// Set sample count.
    #[arg(short = 'c', long)]
    count: Option<i64>,
    /// Set sample interval.
    #[arg(short = 'i', long)]
    interval: Option<i64>,
    /// Set time to backfill from.
    #[arg(short = 'b', long)]
    backfill: Option<String>,
    /// Set time to end generation at, or a number of intervals to run.
    #[arg(short = 'e', long)]
    end: Option<String>,
    /// Generator worker threads (default: all cores). `--generators` is the
    /// eventgen spelling of the same knob.
    #[arg(long, alias = "generators", env = "FIREBOX_THREADS")]
    threads: Option<usize>,
    /// Seed the random generators for reproducible output.
    #[arg(long, env = "FIREBOX_SEED")]
    seed: Option<u64>,
    /// Stop after this many seconds (firebox extension).
    #[arg(long)]
    duration: Option<f64>,
    /// Log throughput every N seconds (firebox extension; 0 disables).
    #[arg(long, default_value = "0")]
    stats: f64,
    /// Path of the Stoker agent socket for outputMode = stoker.
    #[arg(long, env = "STOKER_OUTPUT_SOCKET", default_value = "/tmp/stoker-output.sock")]
    socket: String,
    /// How workers share the Stoker socket.
    #[arg(long, value_enum, env = "FIREBOX_SOCKET_CONNECTIONS", default_value = "per-thread")]
    connections: Connections,
    // Accepted for command-line compatibility with eventgen; they change nothing.
    #[arg(long, hide = true)]
    multiprocess: bool,
    #[arg(long = "outputters", hide = true)]
    _outputters: Option<usize>,
    #[arg(long = "disableOutputQueue", hide = true)]
    _disable_output_queue: bool,
    #[arg(long = "profiler", hide = true)]
    _profiler: bool,
    #[arg(long = "generator-queue-size", hide = true)]
    _generator_queue_size: Option<usize>,
    #[arg(long = "disable-logging", hide = true)]
    _disable_logging: bool,
}

#[derive(Args, Debug)]
struct ValidateArgs {
    configfile: PathBuf,
    #[arg(short = 's', long = "sample")]
    sample: Option<String>,
}

#[derive(Args, Debug)]
struct BenchArgs {
    configfile: PathBuf,
    #[arg(short = 's', long = "sample")]
    sample: Option<String>,
    /// Seconds to run.
    #[arg(long, default_value = "5")]
    seconds: f64,
    #[arg(long, alias = "generators", env = "FIREBOX_THREADS")]
    threads: Option<usize>,
    #[arg(long)]
    seed: Option<u64>,
}

fn main() {
    let cli = Cli::parse();
    let level = match cli.verbosity {
        0 => log::LevelFilter::Error,
        1 => log::LevelFilter::Info,
        2 => log::LevelFilter::Debug,
        _ => log::LevelFilter::Trace,
    };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(level.as_str()))
        .format_timestamp_millis()
        .init();
    let code = match cli.command {
        Command::Generate(a) => run_generate(a),
        Command::Validate(a) => run_validate(a),
        Command::Bench(a) => run_bench(a),
        Command::Version => {
            println!("firebox {}", firebox::VERSION);
            0
        }
    };
    std::process::exit(code);
}

fn install_signal_flag() -> Arc<AtomicBool> {
    let stop = Arc::new(AtomicBool::new(false));
    for sig in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        let _ = signal_hook::flag::register(sig, stop.clone());
    }
    stop
}

fn threads_or_default(t: Option<usize>) -> usize {
    t.filter(|n| *n > 0).unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1))
}

fn run_generate(a: GenerateArgs) -> i32 {
    let override_output = if a.devnull {
        Some("devnull".to_string())
    } else if a.stdout {
        Some("stdout".to_string())
    } else if a.modinput {
        Some("modinput".to_string())
    } else {
        None
    };
    let opts = LoadOptions {
        sample: a.sample.clone(),
        override_count: a.count,
        override_interval: a.interval,
        override_end: a.end.clone(),
        override_backfill: a.backfill.clone(),
        override_output,
    };
    let conf = match conf::load(&a.configfile, &opts) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("firebox: {}", e);
            return 2;
        }
    };
    let out_opts = OutputOptions {
        socket_path: a.socket.clone(),
        socket_connections: match a.connections {
            Connections::PerThread => SocketConnections::PerThread,
            Connections::Single => SocketConnections::Single,
        },
    };
    let outputs = match Outputs::build(&conf.samples, &out_opts) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("firebox: {}", e);
            return 2;
        }
    };
    let seed = a.seed.or_else(|| conf.global_i64("seed").map(|v| v as u64));
    let stop = install_signal_flag();
    let threads = threads_or_default(a.threads);
    log::info!("firebox {} starting with {} generator thread(s)", firebox::VERSION, threads);
    let engine = match Engine::new(
        conf,
        outputs,
        EngineOptions {
            threads,
            seed,
            duration: a.duration.filter(|d| *d > 0.0).map(Duration::from_secs_f64),
            stats_every: if a.stats > 0.0 { Some(Duration::from_secs_f64(a.stats)) } else { None },
        },
        stop,
    ) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("firebox: {}", e);
            return 2;
        }
    };
    match engine.run() {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("firebox: {}", e);
            1
        }
    }
}

fn run_validate(a: ValidateArgs) -> i32 {
    let opts = LoadOptions { sample: a.sample, ..Default::default() };
    let conf = match conf::load(&a.configfile, &opts) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("firebox: {}", e);
            return 2;
        }
    };
    println!("conf: {}", conf.conf_path.display());
    println!("samples: {}", conf.samples.len());
    let mut problems = 0;
    for s in &conf.samples {
        println!("\n[{}] -> {}", s.stanza, s.name);
        println!(
            "  file: {}",
            s.file_path.as_ref().map(|p| p.display().to_string()).unwrap_or_else(|| "(none)".into())
        );
        println!("  sampleDir: {}", s.sample_dir.display());
        for k in [
            "mode",
            "generator",
            "rater",
            "interval",
            "count",
            "earliest",
            "latest",
            "outputMode",
            "perDayVolume",
            "randomizeCount",
            "end",
            "backfill",
            "index",
            "sourcetype",
            "source",
            "host",
        ] {
            if let Some(v) = s.get(k) {
                println!("  {} = {}", k, v);
            }
        }
        for t in &s.tokens {
            let status = match firebox::pyre::Rx::compile(&t.token) {
                Ok(rx) => match rx {
                    firebox::pyre::Rx::Std { .. } => "ok".to_string(),
                    firebox::pyre::Rx::Fancy { .. } => "ok (backtracking engine)".to_string(),
                },
                Err(e) => {
                    problems += 1;
                    format!("ERROR {}", e)
                }
            };
            println!("  token.{}: {} => {} '{}'  [{}]", t.index, t.token, t.replacement_type, t.replacement, status);
        }
        if let Some(p) = &s.file_path {
            match firebox::sample::load_raw(p, s.get("breaker").unwrap_or(firebox::sample::DEFAULT_BREAKER)) {
                Ok(d) => {
                    println!("  events: {} ({} bytes)", d.len(), d.total_size());
                    // report which tokens match the first line
                    if let Some(first) = d.lines.first() {
                        for t in &s.tokens {
                            if let Ok(rx) = firebox::pyre::Rx::compile(&t.token) {
                                let mut spans = Vec::new();
                                rx.find_all(&first.raw, &mut spans);
                                println!("  token.{} matches on line 1: {}", t.index, spans.len());
                            }
                        }
                    }
                }
                Err(e) => {
                    problems += 1;
                    println!("  ERROR loading sample: {}", e);
                }
            }
        }
    }
    if problems > 0 {
        println!("\n{} problem(s)", problems);
        1
    } else {
        println!("\nok");
        0
    }
}

fn run_bench(a: BenchArgs) -> i32 {
    // Rewrite every stanza as a hot loop: interval 1, a large count, devnull,
    // then run for the requested time and report the delivered rate.
    let opts = LoadOptions {
        sample: a.sample,
        override_interval: Some(1),
        override_output: Some("devnull".into()),
        ..Default::default()
    };
    let conf = match conf::load(&a.configfile, &opts) {
        Ok(mut c) => {
            for s in &mut c.samples {
                if s.mode() != "replay" && s.get_f64("perDayVolume").is_none() {
                    // enough work per second that the pool never idles
                    s.set("count", "2000000");
                    s.remove("randomizeCount");
                }
                s.remove("backfill");
                s.remove("end");
                s.remove("delay");
            }
            c
        }
        Err(e) => {
            eprintln!("firebox: {}", e);
            return 2;
        }
    };
    let outputs = match Outputs::build(
        &conf.samples,
        &OutputOptions { socket_path: String::new(), socket_connections: SocketConnections::PerThread },
    ) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("firebox: {}", e);
            return 2;
        }
    };
    let stop = install_signal_flag();
    let threads = threads_or_default(a.threads);
    let engine = match Engine::new(
        conf,
        outputs,
        EngineOptions { threads, seed: a.seed, duration: Some(Duration::from_secs_f64(a.seconds)), stats_every: None },
        stop.clone(),
    ) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("firebox: {}", e);
            return 2;
        }
    };
    let stats = engine.stats();
    let started = Instant::now();
    let rc = engine.run();
    let secs = started.elapsed().as_secs_f64();
    let ev = stats.events.load(Ordering::Relaxed);
    let by = stats.bytes.load(Ordering::Relaxed);
    println!(
        "bench: threads={} events={} bytes={} seconds={:.2} eps={:.0} MB/s={:.2} per_thread_eps={:.0}",
        threads,
        ev,
        by,
        secs,
        ev as f64 / secs,
        by as f64 / 1e6 / secs,
        ev as f64 / secs / threads as f64
    );
    match rc {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("firebox: {}", e);
            1
        }
    }
}
