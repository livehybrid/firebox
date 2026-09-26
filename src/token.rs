//! Token replacement (`lib/eventgentoken.py`).
//!
//! Every token is a compiled regex plus a replacement kind. Per event, a token
//! finds all its matches, computes ONE replacement value and writes it into
//! every match (group 1 when the pattern has one, else the whole match), which
//! is exactly upstream's behaviour, including "on any error leave the text
//! alone".

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

use rand::rngs::SmallRng;
use rand::Rng;

use crate::conf::TokenSpec;
use crate::pyre::{Rx, Span};
use crate::rate::{py_round, RateMaps};
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
    /// Identity for the per-event multivalue cache.
    pub file_id: usize,
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
    /// A replacement that can never work (logged once at load).
    Broken,
}

#[derive(Debug)]
pub struct Token {
    pub rx: Rx,
    pub kind: Kind,
    pub spec: TokenSpec,
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
    pub scratch: String,
    /// The `earliest`/`latest` strings are set (they always are after
    /// layering, but upstream checks).
    pub window_declared: bool,
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
            spans: Vec::new(),
            scratch: String::new(),
            window_declared: true,
        }
    }

    #[inline]
    pub fn begin_event(&mut self) {
        self.mv.clear();
    }
}

impl Token {
    /// Compile a spec. `cwd` resolves relative file-token paths (eventgen uses
    /// `os.path.abspath`, i.e. the process working directory); `sample_dir`
    /// holds integerid state files.
    pub fn compile(spec: &TokenSpec, cwd: &Path, sample_dir: &Path, file_id: usize) -> Result<Token, String> {
        let rx = Rx::compile(&spec.token)?;
        let kind = match spec.replacement_type.as_str() {
            "static" => Kind::Static(spec.replacement.clone()),
            "timestamp" => Kind::Timestamp(Layout::compile(&spec.replacement)),
            "replaytimestamp" => Kind::ReplayTimestamp(Layout::compile(&spec.replacement)),
            "random" => Kind::Random(RandomSpec::parse(&spec.replacement)),
            "rated" => Kind::Rated(RandomSpec::parse(&spec.replacement)),
            "file" | "mvfile" | "seqfile" => {
                match load_file_spec(&spec.replacement, cwd, spec.replacement_type == "seqfile", file_id) {
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

    /// Persist an integerid counter the way `Sample.saveState` does.
    pub fn save_state(&self, sample_dir: &Path) {
        if let Kind::IntegerId(v) = &self.kind {
            let _ = std::fs::write(state_file(sample_dir, &self.spec.token), v.load(Ordering::Relaxed).to_string());
        }
    }

    /// Replace every occurrence in `event`. Returns true when the text changed.
    pub fn replace(&self, event: &mut String, ctx: &mut EventCtx<'_>) -> bool {
        self.rx.find_all(event, &mut ctx.spans);
        if ctx.spans.is_empty() {
            return false;
        }
        let (fs, fe) = ctx.spans[0].target();
        let value: Option<String> = match &self.kind {
            Kind::Static(s) => Some(s.clone()),
            Kind::ReplayTimestamp(layout) => ctx.lt.map(|lt| layout.format(&lt)),
            Kind::Timestamp(layout) => self.timestamp_value(layout, ctx),
            Kind::Random(spec) => random_value(spec, ctx, None),
            Kind::Rated(spec) => {
                let factor = ctx.rate_maps.rated_factor(&ctx.now);
                random_value(spec, ctx, Some(factor))
            }
            Kind::File(f) => file_value(f, ctx),
            Kind::IntegerId(counter) => Some(counter.fetch_add(1, Ordering::Relaxed).to_string()),
            Kind::Broken => None,
        };
        let Some(value) = value else {
            return false; // upstream returns `old`: the text is left alone
        };
        if ctx.spans.len() == 1 && value == event[fs..fe] {
            return false;
        }
        let spans = std::mem::take(&mut ctx.spans);
        let out = &mut ctx.scratch;
        out.clear();
        let mut pos = 0;
        for sp in &spans {
            let (s, e) = sp.target();
            if s < pos {
                continue; // overlapping group spans cannot happen with finditer, but stay safe
            }
            out.push_str(&event[pos..s]);
            out.push_str(&value);
            pos = e;
        }
        out.push_str(&event[pos..]);
        std::mem::swap(event, out);
        ctx.spans = spans;
        true
    }

    fn timestamp_value(&self, layout: &Layout, ctx: &mut EventCtx<'_>) -> Option<String> {
        if !ctx.window_declared {
            log::error!("Earliest or latest specifier were not set; will not replace");
            return None;
        }
        let (Some(et), Some(lt)) = (ctx.et, ctx.lt) else {
            return None;
        };
        if lt.epoch < et.epoch {
            log::error!("Earliest '{}' is greater than latest '{}'; will not replace", et.epoch, lt.epoch);
            return None;
        }
        if !layout.has_specifiers() {
            log::error!("Invalid strptime specifier '{}' detected; will not replace", self.spec.replacement);
            return None;
        }
        // The default generator always supplies a pivot. Replay passes the
        // event time as both bounds; use it directly rather than upstream's
        // sticky per-sample `timestamp` (which froze every replayed event at
        // the first event's time).
        let ts = match ctx.pivot {
            Some(p) => p,
            None => lt,
        };
        Some(layout.format(&ts))
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

fn load_file_spec(replacement: &str, cwd: &Path, sequential: bool, file_id: usize) -> Result<FileSpec, String> {
    let parts: Vec<&str> = replacement.split(':').collect();
    let (path_text, column) = if parts.len() == 1 {
        (replacement.to_string(), 0usize)
    } else {
        match parts[parts.len() - 1].trim().parse::<usize>() {
            Ok(col) if col > 0 => (parts[..parts.len() - 1].join(":"), col),
            _ => (replacement.to_string(), 0),
        }
    };
    let path = path_parser(&path_text, cwd);
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

fn file_value(f: &FileSpec, ctx: &mut EventCtx<'_>) -> Option<String> {
    if f.column > 0 {
        if let Some((_, cols)) = ctx.mv.iter().find(|(id, _)| *id == f.file_id) {
            if f.column > cols.len() {
                log::error!(
                    "Index for column '{}' in replacement file '{}' is out of bounds",
                    f.column,
                    f.path.display()
                );
                return None;
            }
            return Some(cols[f.column - 1].clone());
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
        let picked = if f.column > cols.len() {
            log::error!("Index for column '{}' in replacement file '{}' is out of bounds", f.column, f.path.display());
            None
        } else {
            Some(cols[f.column - 1].clone())
        };
        ctx.mv.push((f.file_id, cols));
        picked
    } else {
        Some(line.clone())
    }
}

fn random_value(spec: &RandomSpec, ctx: &mut EventCtx<'_>, rate_factor: Option<f64>) -> Option<String> {
    let rng = &mut *ctx.rng;
    Some(match spec {
        RandomSpec::Ipv4 => {
            let o: [u8; 4] = rng.random();
            format!("{}.{}.{}.{}", o[0], o[1], o[2], o[3])
        }
        RandomSpec::Ipv6 => {
            let mut s = String::with_capacity(39);
            for i in 0..8 {
                if i > 0 {
                    s.push(':');
                }
                let v: u16 = rng.random();
                s.push_str(&format!("{:x}", v));
            }
            s
        }
        RandomSpec::Mac => {
            let mut s = String::with_capacity(17);
            for i in 0..6 {
                if i > 0 {
                    s.push(':');
                }
                let v: u8 = rng.random();
                s.push_str(&format!("{:02x}", v));
            }
            s
        }
        RandomSpec::Guid => {
            let mut b: [u8; 16] = rng.random();
            b[6] = (b[6] & 0x0f) | 0x40;
            b[8] = (b[8] & 0x3f) | 0x80;
            format!(
                "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]
            )
        }
        RandomSpec::Integer(lo, hi) => {
            if hi < lo {
                log::error!("Start integer {} greater than end integer {}; will not replace", lo, hi);
                return None;
            }
            let mut v = rng.random_range(*lo..=*hi);
            if let Some(f) = rate_factor {
                v = py_round(v as f64 * f);
            }
            v.to_string()
        }
        RandomSpec::Float { lo, hi, prec } => {
            if hi < lo {
                log::error!("Start float {} greater than end float {}; will not replace", lo, hi);
                return None;
            }
            let mut v = py_round_to(rng.random_range(*lo..=*hi), *prec);
            if let Some(f) = rate_factor {
                v = py_round_to(v * f, *prec);
            }
            py_float_str(v)
        }
        RandomSpec::StringN(n) => {
            let mut s = String::with_capacity(*n);
            for _ in 0..*n {
                s.push(URL_SAFE[rng.random_range(0..URL_SAFE.len())] as char);
            }
            s
        }
        RandomSpec::HexN(n) => {
            let mut s = String::with_capacity(*n);
            for _ in 0..*n {
                s.push(HEX_UPPER[rng.random_range(0..16)] as char);
            }
            s
        }
        RandomSpec::List(items) => {
            if items.is_empty() {
                return None;
            }
            items[rng.random_range(0..items.len())].clone()
        }
        RandomSpec::Invalid(_) => return None,
    })
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
        let cases: [(&str, fn(&str) -> bool); 8] = [
            ("integer[10:20]", |v: &str| (10..=20).contains(&v.parse::<i64>().unwrap())),
            ("float[1.50:2.50]", |v: &str| v.contains('.') && v.split('.').nth(1).unwrap().len() <= 2),
            ("string(8)", |v: &str| v.len() == 8 && v.bytes().all(|b| URL_SAFE.contains(&b))),
            ("hex(6)", |v: &str| v.len() == 6 && v.bytes().all(|b| HEX_UPPER.contains(&b))),
            ("guid", |v: &str| v.len() == 36 && v.as_bytes()[14] == b'4'),
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
        assert_eq!(py_float_str(3.14), "3.14");
        assert_eq!(py_round_to(2.675, 2), 2.67); // binary repr rounds down, like Python
        assert_eq!(py_float_str(py_round_to(1.5, 0)), "2.0");
        assert_eq!(url_quote(r"\d+ x"), "%5Cd%2B%20x");
    }
}
