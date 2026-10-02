//! Token replacement (`lib/eventgentoken.py`).
//!
//! Every token is a compiled regex plus a replacement kind. Per event, a token
//! finds all its matches, computes ONE replacement value and writes it into
//! every match (group 1 when the pattern has one, else the whole match), which
//! is exactly upstream's behaviour, including "on any error leave the text
//! alone". The hot path allocates nothing: values are written into per-thread
//! scratch buffers and the event is rebuilt in place.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;

use rand::rngs::SmallRng;
use rand::Rng;

use crate::conf::TokenSpec;
use crate::pyre::{Rx, RxLocal, Span};
use crate::rate::{py_round, RateMaps};
use crate::rotate;
use crate::strftime::{Layout, Ts};

const URL_SAFE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.-~/";
const HEX_UPPER: &[u8] = b"0123456789ABCDEF";

#[derive(Debug, Clone)]
pub enum RandomSpec {
    Ipv4,
    Ipv6,
    Mac,
    Guid,
    Integer(i64, i64),
    Float { lo: f64, hi: f64, prec: usize },
    StringN(usize),
    HexN(usize),
    List(Vec<String>),
    Invalid(String),
}

impl RandomSpec {
    pub fn parse(spec: &str) -> RandomSpec {
        let lower = spec.to_ascii_lowercase();
        match lower.as_str() {
            "ipv4" => return RandomSpec::Ipv4,
            "ipv6" => return RandomSpec::Ipv6,
            "mac" => return RandomSpec::Mac,
            "guid" => return RandomSpec::Guid,
            _ => {}
        }
        if let Some(inner) = bracket(&lower, "integer[") {
            if let Some((a, b)) = inner.split_once(':') {
                if let (Ok(lo), Ok(hi)) = (a.parse::<i64>(), b.parse::<i64>()) {
                    return RandomSpec::Integer(lo, hi);
                }
            }
        }
        if let Some(inner) = bracket(&lower, "float[") {
            if let Some((a, b)) = inner.split_once(':') {
                if let (Ok(lo), Ok(hi)) = (a.parse::<f64>(), b.parse::<f64>()) {
                    // significance = decimals of the START value
                    let prec = a.split_once('.').map(|(_, d)| d.len()).unwrap_or(0);
                    return RandomSpec::Float { lo, hi, prec };
                }
            }
        }
        if let Some(inner) = paren(&lower, "string(") {
            if let Ok(n) = inner.parse::<usize>() {
                return RandomSpec::StringN(n);
            }
        }
        if let Some(inner) = paren(&lower, "hex(") {
            if let Ok(n) = inner.parse::<usize>() {
                return RandomSpec::HexN(n);
            }
        }
        if lower.starts_with("list[") {
            // upstream: list(\[[^\]]+\]) then json.loads on the bracketed text
            if let Some(end) = spec.find(']') {
                let json = &spec[4..=end];
                match serde_json::from_str::<serde_json::Value>(json) {
                    Ok(serde_json::Value::Array(items)) => {
                        return RandomSpec::List(
                            items
                                .into_iter()
                                .map(|v| match v {
                                    serde_json::Value::String(s) => s,
                                    other => other.to_string(),
                                })
                                .collect(),
                        );
                    }
                    _ => return RandomSpec::Invalid(spec.to_string()),
                }
            }
        }
        RandomSpec::Invalid(spec.to_string())
    }
}

fn bracket<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    s.strip_prefix(prefix)?.strip_suffix(']')
}

fn paren<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    s.strip_prefix(prefix)?.strip_suffix(')')
}

#[derive(Debug)]
pub struct FileSpec {
    pub path: PathBuf,
    pub lines: Vec<String>,
    /// 1-based column for `path:N`; 0 = the whole line.
    pub column: usize,
    pub sequential: bool,
    pub seq_counter: AtomicUsize,
    /// Identity for the per-event multivalue cache (one id per distinct file).
    pub file_id: usize,
}

/// A `rotate` token: the format policy, plus the per-stanza rotation state it
/// shares with every other rotate token in the stanza (so a value matched by
/// two patterns rotates as one identity).
#[derive(Debug)]
pub struct RotateArgs {
    pub policy: rotate::Policy,
    pub state: Arc<rotate::State>,
}

#[derive(Debug)]
pub enum Kind {
    Static(String),
    Timestamp(Layout),
    ReplayTimestamp(Layout),
    Random(RandomSpec),
    Rated(RandomSpec),
    File(FileSpec),
    IntegerId(AtomicI64),
    /// Identity rotation: unlike every other kind this evaluates PER MATCH,
    /// because the replacement is a function of the matched text.
    Rotate(RotateArgs),
    /// A replacement that can never work (logged once at load).
    Broken,
}

#[derive(Debug)]
pub struct Token {
    pub rx: Rx,
    pub kind: Kind,
    pub spec: TokenSpec,
}

/// Build a stanza's rotation identity tables from its own sample.
///
/// Walked **line by line in file order, then token by token in conf order,
/// then match by match left to right**. That order defines each value's `k`,
/// and `k` is part of the identity, so firebox and the vendored Python engine
/// must walk it the same way or a pack split across both engines would mint
/// two different identities for one value. It is also why nothing has to be
/// shipped with the pack: every worker derives the same tables from the sample.
///
/// Only `pass` scope reads these. `window` scope derives the identity from the
/// value itself, so it needs no table, and building one would be wasted work on
/// a large sample.
pub fn build_rotation_tables<'a, I>(tokens: &[Token], lines: I) -> rotate::Tables
where
    I: Iterator<Item = &'a str>,
{
    let rotating: Vec<(&RotateArgs, RxLocal)> = tokens
        .iter()
        .filter_map(|t| match &t.kind {
            Kind::Rotate(args) => Some((args, RxLocal::new(&t.rx))),
            _ => None,
        })
        .collect();
    let mut tables = rotate::Tables::default();
    if rotating.is_empty() || rotating.iter().any(|(a, _)| a.state.scope().is_aligned()) {
        return tables;
    }
    let mut rotating = rotating;
    let mut spans: Vec<Span> = Vec::with_capacity(8);
    for line in lines {
        for (args, local) in rotating.iter_mut() {
            local.find_all(line, &mut spans);
            for sp in spans.iter() {
                let (s, e) = sp.target();
                tables.observe_policy(&args.policy, &line[s..e]);
            }
        }
    }
    tables
}

/// Splice a rotated value into every match, each derived from its own text.
fn rotate_spans(event: &mut String, ctx: &mut EventCtx<'_>, args: &RotateArgs) -> bool {
    let Some(at) = ctx.rotate_at else {
        // Not a rotating stanza (or the position was never set): leave the text
        // alone, which emits the stable stand-in.
        return false;
    };
    let EventCtx { spans, scratch, .. } = ctx;
    scratch.clear();
    let mut pos = 0usize;
    let mut changed = false;
    for sp in spans.iter() {
        let (s, e) = sp.target();
        if s < pos {
            continue;
        }
        scratch.push_str(&event[pos..s]);
        match Token::rotate_one(args, &event[s..e], at) {
            Some(v) => {
                changed |= v != event[s..e];
                scratch.push_str(&v);
            }
            None => scratch.push_str(&event[s..e]),
        }
        pos = e;
    }
    scratch.push_str(&event[pos..]);
    if !changed {
        return false;
    }
    std::mem::swap(event, scratch);
    true
}

/// Per-event, per-thread scratch state handed to every token.
pub struct EventCtx<'a> {
    pub rng: &'a mut SmallRng,
    /// The event's pivot time (default generator) or `None` (replay).
    pub pivot: Option<Ts>,
    /// The generation window; timestamp tokens need lt >= et.
    pub et: Option<Ts>,
    pub lt: Option<Ts>,
    /// "now" for rated tokens.
    pub now: Ts,
    pub rate_maps: &'a RateMaps,
    /// Multivalue-file picks made earlier in this event: (file_id, columns).
    pub mv: Vec<(usize, Vec<String>)>,
    pub spans: Vec<Span>,
    /// The rebuilt event (swapped with the input).
    pub scratch: String,
    /// The replacement value being computed for the current token.
    pub value: String,
    /// The `earliest`/`latest` strings are set (they always are after
    /// layering, but upstream checks).
    pub window_declared: bool,
    /// Where this event sits, for `rotate`: its ordinal within the stanza and
    /// its event time. None outside a rotating stanza, which leaves the text
    /// alone rather than guessing a position.
    pub rotate_at: Option<rotate::Position>,
}

impl<'a> EventCtx<'a> {
    pub fn new(rng: &'a mut SmallRng, now: Ts, rate_maps: &'a RateMaps) -> EventCtx<'a> {
        EventCtx {
            rng,
            pivot: None,
            et: None,
            lt: None,
            now,
            rate_maps,
            mv: Vec::new(),
            spans: Vec::with_capacity(8),
            scratch: String::with_capacity(1024),
            value: String::with_capacity(64),
            window_declared: true,
            rotate_at: None,
        }
    }

    #[inline]
    pub fn begin_event(&mut self) {
        self.mv.clear();
    }
}

impl Token {
    /// Compile a spec. `cwd` resolves relative file-token paths (eventgen uses
    /// `os.path.abspath`, i.e. the process working directory; the pack root is
    /// tried as a fallback so `samples/foo.sample` also works when the process
    /// was started elsewhere); `sample_dir` holds integerid state files.
    pub fn compile(spec: &TokenSpec, cwd: &Path, sample_dir: &Path, file_id: usize) -> Result<Token, String> {
        Token::compile_with(spec, cwd, sample_dir, file_id, None)
    }

    /// As [`Token::compile`], with the stanza's rotation state for `rotate`
    /// tokens. Separate so the six non-rotating call sites stay unchanged.
    pub fn compile_with(
        spec: &TokenSpec,
        cwd: &Path,
        sample_dir: &Path,
        file_id: usize,
        rotation: Option<&Arc<rotate::State>>,
    ) -> Result<Token, String> {
        let rx = Rx::compile(&spec.token)?;
        let kind = match spec.replacement_type.as_str() {
            "static" => Kind::Static(spec.replacement.clone()),
            "timestamp" => Kind::Timestamp(Layout::compile(&spec.replacement)),
            "replaytimestamp" => Kind::ReplayTimestamp(Layout::compile(&spec.replacement)),
            "random" => Kind::Random(RandomSpec::parse(&spec.replacement)),
            "rated" => Kind::Rated(RandomSpec::parse(&spec.replacement)),
            "file" | "mvfile" | "seqfile" => {
                let fallback = sample_dir.parent();
                match load_file_spec(&spec.replacement, cwd, fallback, spec.replacement_type == "seqfile", file_id) {
                    Ok(f) => Kind::File(f),
                    Err(e) => {
                        log::error!("{}", e);
                        Kind::Broken
                    }
                }
            }
            "integerid" => {
                let mut start = spec.replacement.trim().parse::<i64>().unwrap_or_else(|_| {
                    log::error!("integerid replacement {:?} is not an integer; starting at 0", spec.replacement);
                    0
                });
                let state = state_file(sample_dir, &spec.token);
                if let Ok(text) = std::fs::read_to_string(&state) {
                    if let Ok(v) = text.trim().parse::<i64>() {
                        start = v;
                    }
                }
                Kind::IntegerId(AtomicI64::new(start))
            }
            "rotate" => match (rotate::Policy::parse(&spec.replacement), rotation) {
                (Ok(policy), Some(state)) => Kind::Rotate(RotateArgs { policy, state: state.clone() }),
                (Err(e), _) => {
                    log::error!("{}; will not replace", e);
                    Kind::Broken
                }
                (Ok(_), None) => {
                    // Cannot happen: the stanza builds the state whenever any
                    // token rotates. Degrade to leaving the text alone, which
                    // emits the stable stand-in rather than the original.
                    log::error!("token {} rotates but the stanza has no rotation state", spec.index);
                    Kind::Broken
                }
            },
            other => {
                log::error!("Unknown replacementType '{}'; will not replace", other);
                Kind::Broken
            }
        };
        if let Kind::Random(RandomSpec::Invalid(v)) | Kind::Rated(RandomSpec::Invalid(v)) = &kind {
            log::error!(
                "Unknown replacement value '{}' for replacementType '{}'; will not replace",
                v,
                spec.replacement_type
            );
        }
        Ok(Token { rx, kind, spec: spec.clone() })
    }

    pub fn is_timestamp(&self) -> bool {
        matches!(self.kind, Kind::Timestamp(_) | Kind::ReplayTimestamp(_))
    }

    /// Rotate every match independently, each from its own matched text.
    ///
    /// Shares the splice loop's shape with the one-value path above but reads
    /// the span's text for each replacement. A span whose text has nothing to
    /// rotate (not in the sample under `pass` scope, or no variable position)
    /// is copied through, so an unexpected value is left as the stable stand-in
    /// rather than replaced with something arbitrary.
    fn rotate_one(args: &RotateArgs, text: &str, at: rotate::Position) -> Option<String> {
        args.state.render(&args.policy, text, at)
    }

    /// Persist an integerid counter the way `Sample.saveState` does.
    pub fn save_state(&self, sample_dir: &Path) {
        if let Kind::IntegerId(v) = &self.kind {
            let _ = std::fs::write(state_file(sample_dir, &self.spec.token), v.load(Ordering::Relaxed).to_string());
        }
    }

    /// Replace every occurrence in `event` using a temporary regex handle
    /// (tests and one-off paths; the workers use [`Token::replace_local`]).
    pub fn replace(&self, event: &mut String, ctx: &mut EventCtx<'_>) -> bool {
        let mut local = RxLocal::new(&self.rx);
        self.replace_local(event, ctx, &mut local)
    }

    /// Replace every occurrence in `event`. Returns true when the text changed.
    pub fn replace_local(&self, event: &mut String, ctx: &mut EventCtx<'_>, local: &mut RxLocal) -> bool {
        local.find_all(event, &mut ctx.spans);
        if ctx.spans.is_empty() {
            return false;
        }
        // Every other kind computes ONE value and splices it into every match.
        // Rotation cannot: the identity is derived from the matched text, so
        // two different ids in one event must rotate to two different values.
        // Branch here so the shared path below stays exactly as it was.
        if let Kind::Rotate(args) = &self.kind {
            return rotate_spans(event, ctx, args);
        }
        ctx.value.clear();
        let ok = match &self.kind {
            Kind::Static(s) => {
                ctx.value.push_str(s);
                true
            }
            Kind::ReplayTimestamp(layout) => match ctx.lt {
                Some(lt) => {
                    layout.write_str(&mut ctx.value, &lt);
                    true
                }
                None => false,
            },
            Kind::Timestamp(layout) => self.timestamp_into(layout, ctx),
            Kind::Random(spec) => random_into(spec, ctx, None),
            Kind::Rated(spec) => {
                let factor = ctx.rate_maps.rated_factor(&ctx.now);
                random_into(spec, ctx, Some(factor))
            }
            Kind::File(f) => file_into(f, ctx),
            Kind::IntegerId(counter) => {
                let _ = write!(ctx.value, "{}", counter.fetch_add(1, Ordering::Relaxed));
                true
            }
            // Handled by the per-match branch above; false keeps the text as
            // it is if that branch is ever bypassed.
            Kind::Rotate(_) => false,
            Kind::Broken => false,
        };
        if !ok {
            return false; // upstream returns `old`: the text is left alone
        }
        let (fs, fe) = ctx.spans[0].target();
        if ctx.spans.len() == 1 && ctx.value == event[fs..fe] {
            return false;
        }
        let EventCtx { spans, scratch, value, .. } = ctx;
        scratch.clear();
        let mut pos = 0;
        for sp in spans.iter() {
            let (s, e) = sp.target();
            if s < pos {
                continue; // overlapping group spans cannot happen with finditer, but stay safe
            }
            scratch.push_str(&event[pos..s]);
            scratch.push_str(value);
            pos = e;
        }
        scratch.push_str(&event[pos..]);
        std::mem::swap(event, scratch);
        true
    }

    fn timestamp_into(&self, layout: &Layout, ctx: &mut EventCtx<'_>) -> bool {
        if !ctx.window_declared {
            log::error!("Earliest or latest specifier were not set; will not replace");
            return false;
        }
        let (Some(et), Some(lt)) = (ctx.et, ctx.lt) else {
            return false;
        };
        if lt.epoch < et.epoch {
            log::error!("Earliest '{}' is greater than latest '{}'; will not replace", et.epoch, lt.epoch);
            return false;
        }
        if !layout.has_specifiers() {
            log::error!("Invalid strptime specifier '{}' detected; will not replace", self.spec.replacement);
            return false;
        }
        // The default generator always supplies a pivot. Replay passes the
        // event time as both bounds; use it directly rather than upstream's
        // sticky per-sample `timestamp` (which froze every replayed event at
        // the first event's time).
        let ts = match ctx.pivot {
            Some(p) => p,
            None => lt,
        };
        layout.write_str(&mut ctx.value, &ts);
        true
    }
}

fn state_file(sample_dir: &Path, token: &str) -> PathBuf {
    sample_dir.join(format!("state.{}", url_quote(token)))
}

/// `urllib.request.pathname2url` / `quote` with `/` safe.
fn url_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-' | b'~' | b'/') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

fn load_file_spec(
    replacement: &str,
    cwd: &Path,
    fallback_root: Option<&Path>,
    sequential: bool,
    file_id: usize,
) -> Result<FileSpec, String> {
    let parts: Vec<&str> = replacement.split(':').collect();
    let (path_text, column) = if parts.len() == 1 {
        (replacement.to_string(), 0usize)
    } else {
        match parts[parts.len() - 1].trim().parse::<usize>() {
            Ok(col) if col > 0 => (parts[..parts.len() - 1].join(":"), col),
            _ => (replacement.to_string(), 0),
        }
    };
    let mut path = path_parser(&path_text, cwd);
    if !path.is_file() {
        if let Some(root) = fallback_root {
            let alt = path_parser(&path_text, root);
            if alt.is_file() {
                log::debug!("file token {} resolved against the pack root {}", path_text, root.display());
                path = alt;
            }
        }
    }
    if !path.is_file() {
        return Err(format!("File '{}' does not exist", path.display()));
    }
    let text = crate::sample::read_text(&path).map_err(|e| e.to_string())?;
    // readlines(): every line including empty ones; the pick strips whitespace
    let mut lines: Vec<String> = Vec::new();
    let mut start = 0;
    let bytes = text.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        if b == b'\n' {
            lines.push(text[start..i].trim().to_string());
            start = i + 1;
        }
    }
    if start < text.len() {
        lines.push(text[start..].trim().to_string());
    }
    if lines.is_empty() {
        return Err(format!("Replacement file '{}' is empty; will not replace", path.display()));
    }
    Ok(FileSpec { path, lines, column, sequential, seq_counter: AtomicUsize::new(0), file_id })
}

/// `Sample.pathParser` + `os.path.abspath`: backslashes to slashes, `$VAR`
/// segments from the environment, resolved against the working directory.
pub fn path_parser(path: &str, cwd: &Path) -> PathBuf {
    let normalised = path.replace('\\', "/");
    let mut segments: Vec<String> = Vec::new();
    for seg in normalised.split('/') {
        let key = seg.trim_start_matches('$');
        if !key.is_empty() && seg.starts_with('$') {
            if let Ok(v) = std::env::var(key) {
                segments.push(v);
                continue;
            }
        }
        if let Ok(v) = std::env::var(seg) {
            if !seg.is_empty() && seg.chars().all(|c| c.is_ascii_uppercase() || c == '_') {
                segments.push(v);
                continue;
            }
        }
        segments.push(seg.to_string());
    }
    let joined = segments.join("/");
    let p = PathBuf::from(&joined);
    let abs = if p.is_absolute() { p } else { cwd.join(p) };
    normpath(&abs)
}

/// `os.path.normpath` without touching the filesystem.
fn normpath(p: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for c in p.components() {
        match c {
            std::path::Component::ParentDir => {
                if !out.pop() {
                    out.push("..");
                }
            }
            std::path::Component::CurDir => {}
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// Write the file token's value into `ctx.value`.
fn file_into(f: &FileSpec, ctx: &mut EventCtx<'_>) -> bool {
    if f.column > 0 {
        if let Some((_, cols)) = ctx.mv.iter().find(|(id, _)| *id == f.file_id) {
            if f.column > cols.len() {
                log::error!(
                    "Index for column '{}' in replacement file '{}' is out of bounds",
                    f.column,
                    f.path.display()
                );
                return false;
            }
            ctx.value.push_str(&cols[f.column - 1]);
            return true;
        }
    }
    let line = if f.sequential {
        let i = f.seq_counter.fetch_add(1, Ordering::Relaxed);
        &f.lines[i % f.lines.len()]
    } else {
        let i = ctx.rng.random_range(0..f.lines.len());
        &f.lines[i]
    };
    if f.column > 0 {
        let cols: Vec<String> = line.split(',').map(str::to_string).collect();
        let ok = if f.column > cols.len() {
            log::error!("Index for column '{}' in replacement file '{}' is out of bounds", f.column, f.path.display());
            false
        } else {
            ctx.value.push_str(&cols[f.column - 1]);
            true
        };
        ctx.mv.push((f.file_id, cols));
        ok
    } else {
        ctx.value.push_str(line);
        true
    }
}

/// Write a random/rated value into `ctx.value`.
fn random_into(spec: &RandomSpec, ctx: &mut EventCtx<'_>, rate_factor: Option<f64>) -> bool {
    let EventCtx { rng, value, .. } = ctx;
    let rng: &mut SmallRng = rng;
    match spec {
        RandomSpec::Ipv4 => {
            let o: [u8; 4] = rng.random();
            let _ = write!(value, "{}.{}.{}.{}", o[0], o[1], o[2], o[3]);
        }
        RandomSpec::Ipv6 => {
            for i in 0..8 {
                if i > 0 {
                    value.push(':');
                }
                let v: u16 = rng.random();
                let _ = write!(value, "{:x}", v);
            }
        }
        RandomSpec::Mac => {
            for i in 0..6 {
                if i > 0 {
                    value.push(':');
                }
                let v: u8 = rng.random();
                let _ = write!(value, "{:02x}", v);
            }
        }
        RandomSpec::Guid => {
            let mut b: [u8; 16] = rng.random();
            b[6] = (b[6] & 0x0f) | 0x40;
            b[8] = (b[8] & 0x3f) | 0x80;
            for (i, byte) in b.iter().enumerate() {
                if matches!(i, 4 | 6 | 8 | 10) {
                    value.push('-');
                }
                let _ = write!(value, "{:02x}", byte);
            }
        }
        RandomSpec::Integer(lo, hi) => {
            if hi < lo {
                log::error!("Start integer {} greater than end integer {}; will not replace", lo, hi);
                return false;
            }
            let mut v = rng.random_range(*lo..=*hi);
            if let Some(f) = rate_factor {
                v = py_round(v as f64 * f);
            }
            let _ = write!(value, "{}", v);
        }
        RandomSpec::Float { lo, hi, prec } => {
            if hi < lo {
                log::error!("Start float {} greater than end float {}; will not replace", lo, hi);
                return false;
            }
            let mut v = py_round_to(rng.random_range(*lo..=*hi), *prec);
            if let Some(f) = rate_factor {
                v = py_round_to(v * f, *prec);
            }
            value.push_str(&py_float_str(v));
        }
        RandomSpec::StringN(n) => {
            for _ in 0..*n {
                value.push(URL_SAFE[rng.random_range(0..URL_SAFE.len())] as char);
            }
        }
        RandomSpec::HexN(n) => {
            for _ in 0..*n {
                value.push(HEX_UPPER[rng.random_range(0..16)] as char);
            }
        }
        RandomSpec::List(items) => {
            if items.is_empty() {
                return false;
            }
            value.push_str(&items[rng.random_range(0..items.len())]);
        }
        RandomSpec::Invalid(_) => return false,
    }
    true
}

/// Python `round(x, ndigits)`: correctly rounded on the exact binary value
/// (so `round(2.675, 2)` is 2.67), which is what Rust's precision formatting
/// does too.
pub fn py_round_to(x: f64, prec: usize) -> f64 {
    if !x.is_finite() {
        return x;
    }
    format!("{:.*}", prec, x).parse::<f64>().unwrap_or(x)
}

/// Python `str(float)`: shortest round-trip repr, always with a fraction.
pub fn py_float_str(v: f64) -> String {
    if v.is_finite() && v.fract() == 0.0 && v.abs() < 1e16 {
        format!("{:.1}", v)
    } else {
        format!("{}", v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    fn ctx<'a>(rng: &'a mut SmallRng, maps: &'a RateMaps) -> EventCtx<'a> {
        let now = Ts::from_epoch(1_800_000_000, 0).unwrap();
        let mut c = EventCtx::new(rng, now, maps);
        c.pivot = Some(now);
        c.et = Some(now);
        c.lt = Some(now);
        c
    }

    fn spec(token: &str, kind: &str, rep: &str) -> TokenSpec {
        TokenSpec { index: 0, token: token.into(), replacement_type: kind.into(), replacement: rep.into() }
    }

    fn tok(token: &str, kind: &str, rep: &str) -> Token {
        Token::compile(&spec(token, kind, rep), Path::new("/"), Path::new("/tmp"), 0).unwrap()
    }

    #[test]
    fn static_and_group_semantics() {
        let mut rng = SmallRng::seed_from_u64(7);
        let maps = RateMaps::default();
        let mut c = ctx(&mut rng, &maps);
        let t = tok(r"status=(\d{3})", "static", "999");
        let mut e = "a status=200 b status=404".to_string();
        assert!(t.replace(&mut e, &mut c));
        assert_eq!(e, "a status=999 b status=999");
        let t = tok("##X##", "static", "Y");
        let mut e = "##X## and ##X##".to_string();
        t.replace(&mut e, &mut c);
        assert_eq!(e, "Y and Y");
    }

    #[test]
    fn same_value_for_every_match() {
        let mut rng = SmallRng::seed_from_u64(7);
        let maps = RateMaps::default();
        let mut c = ctx(&mut rng, &maps);
        let t = tok(r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}", "random", "ipv4");
        let mut e = "src=1.1.1.1 dst=2.2.2.2".to_string();
        t.replace(&mut e, &mut c);
        let ips: Vec<&str> = e.split(' ').map(|kv| kv.split('=').nth(1).unwrap()).collect();
        assert_eq!(ips[0], ips[1]);
        assert!(ips[0].split('.').all(|o| o.parse::<u8>().is_ok()));
    }

    #[test]
    fn random_kinds() {
        let mut rng = SmallRng::seed_from_u64(3);
        let maps = RateMaps::default();
        let mut c = ctx(&mut rng, &maps);
        type Check = fn(&str) -> bool;
        let cases: [(&str, Check); 8] = [
            ("integer[10:20]", |v: &str| (10..=20).contains(&v.parse::<i64>().unwrap())),
            ("float[1.50:2.50]", |v: &str| v.contains('.') && v.split('.').nth(1).unwrap().len() <= 2),
            ("string(8)", |v: &str| v.len() == 8 && v.bytes().all(|b| URL_SAFE.contains(&b))),
            ("hex(6)", |v: &str| v.len() == 6 && v.bytes().all(|b| HEX_UPPER.contains(&b))),
            ("guid", |v: &str| v.len() == 36 && v.as_bytes()[14] == b'4' && v.matches('-').count() == 4),
            ("ipv6", |v: &str| v.split(':').count() == 8),
            ("mac", |v: &str| v.len() == 17),
            ("list[\"a\",\"b\"]", |v: &str| v == "a" || v == "b"),
        ];
        for (spec_text, check) in cases {
            let t = tok("XX", "random", spec_text);
            for _ in 0..20 {
                let mut e = "XX".to_string();
                t.replace(&mut e, &mut c);
                assert!(check(&e), "{} -> {}", spec_text, e);
            }
        }
        let t = tok("XX", "random", "integer[5:1]");
        let mut e = "XX".to_string();
        assert!(!t.replace(&mut e, &mut c));
        assert_eq!(e, "XX");
    }

    #[test]
    fn timestamp_uses_pivot_and_rejects_bad_format() {
        let mut rng = SmallRng::seed_from_u64(3);
        let maps = RateMaps::default();
        let mut c = ctx(&mut rng, &maps);
        let t = tok(r"\d{4}-\d{2}-\d{2}", "timestamp", "%Y-%m-%d");
        let mut e = "on 1999-01-01 x".to_string();
        t.replace(&mut e, &mut c);
        let expect = Layout::compile("%Y-%m-%d").format(&c.pivot.unwrap());
        assert_eq!(e, format!("on {} x", expect));
        let t = tok(r"\d{4}", "timestamp", "nope");
        let mut e = "1999".to_string();
        assert!(!t.replace(&mut e, &mut c));
    }

    #[test]
    fn file_and_mvfile_tokens() {
        let dir = std::env::temp_dir().join(format!("firebox-tok-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("codes.sample"), "200\n404\n").unwrap();
        std::fs::write(dir.join("mv.sample"), "alice,london\nbob,paris\n").unwrap();
        let mut rng = SmallRng::seed_from_u64(9);
        let maps = RateMaps::default();
        let mut c = ctx(&mut rng, &maps);
        let t = Token::compile(&spec("CODE", "file", "codes.sample"), &dir, &dir, 1).unwrap();
        for _ in 0..10 {
            let mut e = "CODE".into();
            t.replace(&mut e, &mut c);
            assert!(e == "200" || e == "404");
        }
        let t1 = Token::compile(&spec("NAME", "mvfile", "mv.sample:1"), &dir, &dir, 2).unwrap();
        let t2 = Token::compile(&spec("CITY", "mvfile", "mv.sample:2"), &dir, &dir, 2).unwrap();
        for _ in 0..10 {
            c.begin_event();
            let mut e = "NAME CITY".to_string();
            t1.replace(&mut e, &mut c);
            t2.replace(&mut e, &mut c);
            assert!(e == "alice london" || e == "bob paris", "{}", e);
        }
        let seq = Token::compile(&spec("CODE", "seqfile", "codes.sample"), &dir, &dir, 3).unwrap();
        let mut outs = Vec::new();
        for _ in 0..3 {
            let mut e = "CODE".into();
            seq.replace(&mut e, &mut c);
            outs.push(e);
        }
        assert_eq!(outs, vec!["200", "404", "200"]);
        // pack-root fallback: cwd elsewhere, file under <sample_dir>/../samples
        let pack = dir.join("pack");
        std::fs::create_dir_all(pack.join("samples")).unwrap();
        std::fs::write(pack.join("samples/codes.sample"), "7\n").unwrap();
        let t = Token::compile(&spec("CODE", "file", "samples/codes.sample"), Path::new("/"), &pack.join("samples"), 4)
            .unwrap();
        let mut e = "CODE".into();
        t.replace(&mut e, &mut c);
        assert_eq!(e, "7");
    }

    #[test]
    fn integerid_increments() {
        let mut rng = SmallRng::seed_from_u64(1);
        let maps = RateMaps::default();
        let mut c = ctx(&mut rng, &maps);
        let t = tok("ID", "integerid", "41");
        let mut a = "ID".to_string();
        let mut b = "ID".to_string();
        t.replace(&mut a, &mut c);
        t.replace(&mut b, &mut c);
        assert_eq!((a.as_str(), b.as_str()), ("41", "42"));
    }

    #[test]
    fn python_float_formatting() {
        assert_eq!(py_float_str(42.0), "42.0");
        assert_eq!(py_float_str(3.25), "3.25");
        assert_eq!(py_round_to(2.675, 2), 2.67); // binary repr rounds down, like Python
        assert_eq!(py_float_str(py_round_to(1.5, 0)), "2.0");
        assert_eq!(url_quote(r"\d+ x"), "%5Cd%2B%20x");
    }
}
