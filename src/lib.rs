//! Firebox: a fast, multi-threaded, eventgen.conf-compatible event generator.
//!
//! The crate mirrors the behaviour of Splunk's `splunk_eventgen generate`
//! (config layering, token replacement, raters, generators and outputs) with
//! precompiled regexes and formats, per-thread RNGs and batched output, so a
//! single process saturates every core instead of one GIL-bound thread.

pub mod clock;
pub mod conf;
pub mod engine;
pub mod envelope;
pub mod format;
pub mod gen;
pub mod ini;
pub mod output;
pub mod pyre;
pub mod rate;
pub mod rotate;
pub mod sample;
pub mod strftime;
pub mod strptime;
pub mod timeparse;
pub mod token;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
