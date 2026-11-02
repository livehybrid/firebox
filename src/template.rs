//! Precompiled sample lines: literal text plus value slots.
//!
//! The token path ([`crate::prefilter::apply_tokens`]) runs every token that
//! can match an event as a regex search over the whole event, then rebuilds
//! the event to splice its value in. On a pack builder pack (one token per
//! field, forty to hundreds of fields per event) that is ~70% of generation
//! time, and it is the same work every time a line is used: the sample line
//! never changes, only the values written into it.
//!
//! So each line's matches are found once, at load, against the line as it is
//! in the sample. The line becomes a list of literal ranges and slots (the
//! group-1 span of a match, or the whole match), and per event each token
//! that has a slot computes its one value, in token order, and the event is
//! written in a single pass. No regex runs per event.
//!
//! **Semantics.** Every token matches the ORIGINAL line. eventgen instead runs
//! the tokens in sequence over text the earlier ones already rewrote, so the
//! two differ when a replacement value creates, removes or moves a later
//! token's match (a static token that inserts `##placeholder##` for a later
//! token to replace is the classic case). Three guards keep that from
//! changing a pack's output silently:
//!
//! - a line where two tokens' targets overlap is not templated (which value
//!   would win depends on the order) and keeps the sequential path;
//! - at load, as many lines as fit in [`CHECK_BUDGET`] are rendered both ways
//!   with the same seed, and any difference turns templates off for the whole
//!   stanza, with a warning naming the line;
//! - `templates = false` in a stanza, or `FIREBOX_TEMPLATES=0`, turns them off.
//!
//! Random draws happen in the same order as on the sequential path (tokens in
//! order, one value per token per event, none for a token that does not
//! match), so where the two agree they agree byte for byte under a seed.

use std::time::{Duration, Instant};

use rand::rngs::SmallRng;
use rand::SeedableRng;

use crate::prefilter::TokenPrefilter;
use crate::pyre::{RxLocal, Span};
use crate::rate::RateMaps;
use crate::rotate;
use crate::sample::Line;
use crate::strftime::Ts;
use crate::token::{EventCtx, Kind, Token};

/// How long the load-time comparison may run per stanza. Each line is checked
/// at most once, in order, until the budget runs out; the first line always is.
pub const CHECK_BUDGET: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Seg {
    /// A byte range of the original line, copied as is.
    Lit(u32, u32),
    /// A token's target: `local` indexes [`Template::toks`]; `s..e` is the
    /// original text, kept for a value that fails and for rotation.
    Slot { local: u32, s: u32, e: u32 },
}

/// One sample line, precompiled.
#[derive(Debug)]
pub struct Template {
    segs: Vec<Seg>,
    /// The tokens with at least one slot, ascending (the order values are drawn).
    toks: Vec<u32>,
}

/// Per-thread scratch for [`Template::render`].
#[derive(Debug, Default)]
pub struct Scratch {
    vals: String,
    ranges: Vec<Option<(u32, u32)>>,
}

/// A stanza's templates, one per sample line (None = that line keeps the
/// sequential path).
#[derive(Debug)]
pub struct Templates {
    lines: Vec<Option<Template>>,
}

impl Template {
    /// The template for `raw`, or None when two tokens' targets overlap.
    /// `prefilter`, when present, skips the tokens that cannot match.
    pub fn build(
        raw: &str,
        tokens: &[Token],
        locals: &mut [RxLocal],
        prefilter: Option<&TokenPrefilter>,
        present: &mut Vec<bool>,
    ) -> Option<Template> {
        if let Some(pf) = prefilter {
            pf.scan(raw, present);
        }
        let mut hits: Vec<(usize, usize, u32)> = Vec::new();
        let mut spans: Vec<Span> = Vec::new();
        for (ti, (_, local)) in tokens.iter().zip(locals.iter_mut()).enumerate() {
            if let Some(pf) = prefilter {
                if pf.is_gated(ti) && !present[ti] {
                    continue;
                }
            }
            local.find_all(raw, &mut spans);
            let mut pos = 0usize;
            for sp in &spans {
                let (s, e) = sp.target();
                if s < pos {
                    continue; // the splice skips these too
                }
                hits.push((s, e, ti as u32));
                pos = e;
            }
        }
        hits.sort_unstable();
        let mut toks: Vec<u32> = hits.iter().map(|h| h.2).collect();
        toks.sort_unstable();
        toks.dedup();
        let mut segs = Vec::with_capacity(hits.len() * 2 + 1);
        let mut pos = 0usize;
        let mut prev: Option<(usize, u32)> = None;
        for &(s, e, ti) in &hits {
            if s < pos {
                return None; // overlapping targets
            }
            if let Some((ps, pt)) = prev {
                // Two empty targets at one position, or an empty target right
                // where another token's starts: order would decide the text.
                if ps == s && pt != ti {
                    return None;
                }
            }
            if s > pos {
                segs.push(Seg::Lit(pos as u32, s as u32));
            }
            let local = toks.binary_search(&ti).expect("token listed") as u32;
            segs.push(Seg::Slot { local, s: s as u32, e: e as u32 });
            pos = e;
            prev = Some((s, ti));
        }
        if pos < raw.len() {
            segs.push(Seg::Lit(pos as u32, raw.len() as u32));
        }
        Some(Template { segs, toks })
    }

    /// Write the event for `raw` (the line this template was built from) into `out`.
    pub fn render(&self, raw: &str, tokens: &[Token], ctx: &mut EventCtx<'_>, sc: &mut Scratch, out: &mut String) {
        sc.vals.clear();
        sc.ranges.clear();
        for &ti in &self.toks {
            let t = &tokens[ti as usize];
            if matches!(t.kind, Kind::Rotate(_)) {
                sc.ranges.push(None); // per slot, below
                continue;
            }
            if t.value_into(ctx) {
                let a = sc.vals.len() as u32;
                sc.vals.push_str(&ctx.value);
                sc.ranges.push(Some((a, sc.vals.len() as u32)));
            } else {
                sc.ranges.push(None); // upstream leaves the text alone
            }
        }
        out.clear();
        for seg in &self.segs {
            match *seg {
                Seg::Lit(s, e) => out.push_str(&raw[s as usize..e as usize]),
                Seg::Slot { local, s, e } => {
                    let orig = &raw[s as usize..e as usize];
                    match sc.ranges[local as usize] {
                        Some((a, b)) => out.push_str(&sc.vals[a as usize..b as usize]),
                        None => match (&tokens[self.toks[local as usize] as usize].kind, ctx.rotate_at) {
                            (Kind::Rotate(args), Some(at)) => match Token::rotate_one(args, orig, at) {
                                Some(v) => out.push_str(&v),
                                None => out.push_str(orig),
                            },
                            _ => out.push_str(orig),
                        },
                    }
                }
            }
        }
    }

    pub fn slots(&self) -> usize {
        self.segs.iter().filter(|s| matches!(s, Seg::Slot { .. })).count()
    }
}

impl Templates {
    /// Templates for a stanza's lines, checked against the sequential path,
    /// or None when nothing would be templated or the check found a difference.
    pub fn build(
        name: &str,
        lines: &[Line],
        tokens: &[Token],
        prefilter: Option<&TokenPrefilter>,
        rate_maps: &RateMaps,
        rotating: bool,
    ) -> Option<Templates> {
        if tokens.is_empty() || lines.is_empty() {
            return None;
        }
        let built = build_all(lines, tokens, prefilter);
        let mut locals: Vec<RxLocal> = tokens.iter().map(|t| RxLocal::new(&t.rx)).collect();
        let templated = built.iter().filter(|t| t.is_some()).count();
        if templated == 0 {
            log::info!("sample '{}': no line can be templated; using the token path", name);
            return None;
        }
        let templates = Templates { lines: built };
        match templates.check(lines, tokens, &mut locals, prefilter, rate_maps, rotating, CHECK_BUDGET) {
            Ok(checked) => {
                log::info!(
                    "sample '{}': {} of {} line(s) templated, {} checked against the token path",
                    name,
                    templated,
                    lines.len(),
                    checked
                );
                Some(templates)
            }
            Err(line) => {
                log::warn!(
                    "sample '{}': line {} renders differently from a template (a token matches text another \
                     token wrote); templates are off for this sample",
                    name,
                    line + 1
                );
                None
            }
        }
    }

    pub fn get(&self, line: usize) -> Option<&Template> {
        self.lines.get(line).and_then(Option::as_ref)
    }

    /// Render lines both ways with the same seed and state until `budget` is
    /// spent. Ok(lines checked), or Err(index of the first line that differs).
    #[allow(clippy::too_many_arguments)]
    fn check(
        &self,
        lines: &[Line],
        tokens: &[Token],
        locals: &mut [RxLocal],
        prefilter: Option<&TokenPrefilter>,
        rate_maps: &RateMaps,
        rotating: bool,
        budget: Duration,
    ) -> Result<usize, usize> {
        let started = Instant::now();
        let epoch = 1_767_225_600; // 2026-01-01T00:00:00Z; any fixed time will do
        let (Some(now), Some(et), Some(pivot)) =
            (Ts::from_epoch(epoch, 0), Ts::from_epoch(epoch - 3600, 0), Ts::from_epoch(epoch - 1800, 123_456))
        else {
            return Ok(0);
        };
        let saved: Vec<Option<i64>> = tokens.iter().map(Token::side_state).collect();
        let restore = || {
            for (t, v) in tokens.iter().zip(&saved) {
                if let Some(v) = v {
                    t.set_side_state(*v);
                }
            }
        };
        let mut present = Vec::new();
        let mut sc = Scratch::default();
        let (mut a, mut b) = (String::new(), String::new());
        let mut checked = 0usize;
        let mut result = Ok(0);
        for (i, line) in lines.iter().enumerate() {
            if checked > 0 && started.elapsed() > budget {
                break;
            }
            let Some(t) = self.get(i) else { continue };
            for pass in 0..2 {
                let seed = check_seed(i as u64, pass);
                let mut rng = SmallRng::seed_from_u64(seed);
                let mut ctx = check_ctx(&mut rng, now, et, pivot, rate_maps, rotating, i);
                a.clear();
                a.push_str(&line.raw);
                crate::prefilter::apply_tokens(tokens, locals, prefilter, &mut present, &mut a, &mut ctx);
                restore();
                let mut rng = SmallRng::seed_from_u64(seed);
                let mut ctx = check_ctx(&mut rng, now, et, pivot, rate_maps, rotating, i);
                t.render(&line.raw, tokens, &mut ctx, &mut sc, &mut b);
                restore();
                if a != b {
                    result = Err(i);
                    break;
                }
            }
            if result.is_err() {
                break;
            }
            checked += 1;
            result = Ok(checked);
        }
        result
    }
}

/// Every line's template, the lines split across the cores: each search is
/// cold the first time a regex runs, and a large sample runs tens of
/// thousands of them.
fn build_all(lines: &[Line], tokens: &[Token], prefilter: Option<&TokenPrefilter>) -> Vec<Option<Template>> {
    let threads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1).min(16);
    let per = lines.len().div_ceil(threads).max(64);
    let build_chunk = |chunk: &[Line]| -> Vec<Option<Template>> {
        let mut locals: Vec<RxLocal> = tokens.iter().map(|t| RxLocal::new(&t.rx)).collect();
        let mut present = Vec::new();
        chunk.iter().map(|l| Template::build(&l.raw, tokens, &mut locals, prefilter, &mut present)).collect()
    };
    if lines.len() <= per {
        return build_chunk(lines);
    }
    std::thread::scope(|scope| {
        let handles: Vec<_> = lines.chunks(per).map(|c| scope.spawn(move || build_chunk(c))).collect();
        handles.into_iter().flat_map(|h| h.join().expect("template build thread")).collect()
    })
}

#[inline]
fn check_seed(line: u64, pass: u64) -> u64 {
    line.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ pass.wrapping_add(0x5EED)
}

fn check_ctx<'a>(
    rng: &'a mut SmallRng,
    now: Ts,
    et: Ts,
    pivot: Ts,
    rate_maps: &'a RateMaps,
    rotating: bool,
    line: usize,
) -> EventCtx<'a> {
    let mut ctx = EventCtx::new(rng, now, rate_maps);
    ctx.et = Some(et);
    ctx.lt = Some(now);
    ctx.pivot = Some(pivot);
    if rotating {
        ctx.rotate_at = Some(rotate::Position::new(line as u64, pivot.epoch));
    }
    ctx.begin_event();
    ctx
}

/// Write the event for sample line `idx` into `out`: from its template when it
/// has one, else through the token path.
#[allow(clippy::too_many_arguments)]
pub fn render_line(
    tokens: &[Token],
    templates: Option<&Templates>,
    idx: usize,
    raw: &str,
    locals: &mut [RxLocal],
    prefilter: Option<&TokenPrefilter>,
    present: &mut Vec<bool>,
    scratch: &mut Scratch,
    out: &mut String,
    ctx: &mut EventCtx<'_>,
) {
    if let Some(t) = templates.and_then(|ts| ts.get(idx)) {
        t.render(raw, tokens, ctx, scratch, out);
        return;
    }
    out.clear();
    out.push_str(raw);
    crate::prefilter::apply_tokens(tokens, locals, prefilter, present, out, ctx);
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::conf::TokenSpec;

    fn tok(token: &str, kind: &str, rep: &str) -> Token {
        let spec = TokenSpec {
            index: 0,
            token: token.into(),
            replacement_type: kind.into(),
            replacement: rep.into(),
            ..Default::default()
        };
        Token::compile(&spec, Path::new("/"), Path::new("/nonexistent"), 0).unwrap()
    }

    fn line(raw: &str) -> Line {
        Line { raw: raw.into(), size: raw.len() + 1, meta: None, time_field: None }
    }

    fn template(raw: &str, tokens: &[Token]) -> Option<Template> {
        let mut locals: Vec<RxLocal> = tokens.iter().map(|t| RxLocal::new(&t.rx)).collect();
        Template::build(raw, tokens, &mut locals, None, &mut Vec::new())
    }

    /// Render `raw` both ways from the same seed.
    fn both(raw: &str, tokens: &[Token], seed: u64) -> (String, String) {
        let t = template(raw, tokens).expect("templated");
        let maps = RateMaps::default();
        let now = Ts::from_epoch(1_800_000_000, 0).unwrap();
        let et = Ts::from_epoch(1_799_999_000, 0).unwrap();
        let mut locals: Vec<RxLocal> = tokens.iter().map(|t| RxLocal::new(&t.rx)).collect();
        let saved: Vec<Option<i64>> = tokens.iter().map(Token::side_state).collect();
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut ctx = check_ctx(&mut rng, now, et, now, &maps, false, 0);
        let mut a = raw.to_string();
        crate::prefilter::apply_tokens(tokens, &mut locals, None, &mut Vec::new(), &mut a, &mut ctx);
        for (t, v) in tokens.iter().zip(&saved) {
            if let Some(v) = v {
                t.set_side_state(*v);
            }
        }
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut ctx = check_ctx(&mut rng, now, et, now, &maps, false, 0);
        let mut b = String::new();
        t.render(raw, tokens, &mut ctx, &mut Scratch::default(), &mut b);
        (a, b)
    }

    fn mixed() -> Vec<Token> {
        vec![
            tok(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", "timestamp", "%Y-%m-%dT%H:%M:%S"),
            tok(r#""user"\s*:\s*"([^"]*)""#, "random", "guid"),
            tok(r#""bytes"\s*:\s*(\d+)"#, "random", "integer[0:99999]"),
            tok(r#""host"\s*:\s*"([^"]*)""#, "random", "ipv4"),
            tok(r#""kind"\s*:\s*"([^"]*)""#, "static", "fixed"),
            tok(r#""seq"\s*:\s*(\d+)"#, "integerid", "100"),
            tok(r#""absent"\s*:\s*"([^"]*)""#, "random", "hex(8)"),
        ]
    }

    #[test]
    fn matches_the_token_path_byte_for_byte() {
        let tokens = mixed();
        let raw = r#"{"t":"2026-01-01T00:00:00","user":"u1","bytes":12,"host":"h","kind":"k","seq":1,"user":"u1"}"#;
        for seed in 0..50 {
            let (a, b) = both(raw, &tokens, seed);
            assert_eq!(a, b);
            assert_ne!(a, raw);
        }
    }

    #[test]
    fn a_token_with_no_match_draws_nothing() {
        let tokens = mixed();
        let t = template(r#"{"bytes":1}"#, &tokens).unwrap();
        assert_eq!(t.toks, vec![2]);
        assert_eq!(t.slots(), 1);
    }

    #[test]
    fn every_match_of_a_token_shares_one_value() {
        let tokens = vec![tok("ID", "random", "guid")];
        let (_, b) = both("ID and ID", &tokens, 3);
        let parts: Vec<&str> = b.split(" and ").collect();
        assert_eq!(parts[0], parts[1]);
    }

    #[test]
    fn overlapping_targets_keep_the_token_path() {
        let tokens = vec![tok(r"a=(\w+)", "static", "x"), tok(r"(\w+)$", "static", "y")];
        assert!(template("a=bc", &tokens).is_none());
        assert!(template("a=bc d", &tokens).is_some());
    }

    #[test]
    fn a_failed_value_leaves_the_text() {
        // A timestamp layout with no specifiers is refused, as upstream does.
        let tokens = vec![tok("X", "timestamp", "no specifiers")];
        let (a, b) = both("X", &tokens, 1);
        assert_eq!(a, "X");
        assert_eq!(b, "X");
    }

    #[test]
    fn column_patterns_are_templated() {
        let tokens =
            vec![tok(r"^([^,\n]*)", "random", "integer[0:9]"), tok(r"^(?:[^,\n]*,){2}([^,\n]*)", "static", "C")];
        let (a, b) = both("a,b,c,d", &tokens, 5);
        assert_eq!(a, b);
        assert!(b.ends_with(",b,C,d"));
    }

    #[test]
    fn chained_tokens_switch_templates_off() {
        // The classic eventgen chain: one token writes a placeholder that a
        // later one replaces. Matching the original line would miss it.
        let tokens = vec![tok("@@USER@@", "static", "##NAME##"), tok("##NAME##", "static", "alice")];
        let lines = vec![line("hello @@USER@@")];
        assert!(Templates::build("t", &lines, &tokens, None, &RateMaps::default(), false).is_none());
        let unchained = vec![tok("@@USER@@", "static", "bob"), tok("##NAME##", "static", "alice")];
        assert!(Templates::build("t", &lines, &unchained, None, &RateMaps::default(), false).is_some());
    }

    #[test]
    fn the_check_leaves_counters_where_they_were() {
        let tokens = vec![tok("N", "integerid", "41")];
        let lines = vec![line("N"), line("N N")];
        assert!(Templates::build("t", &lines, &tokens, None, &RateMaps::default(), false).is_some());
        assert_eq!(tokens[0].side_state(), Some(41));
    }

    #[test]
    fn the_prefilter_gives_the_same_template() {
        let mut tokens = mixed();
        for i in 0..20 {
            tokens.push(tok(&format!(r#""f{}"\s*:\s*"([^"]*)""#, i), "random", "hex(4)"));
        }
        let pf = TokenPrefilter::build(&tokens).expect("enough gated tokens");
        let raw = r#"{"user":"u","f3":"x","f17":"y","bytes":5}"#;
        let mut locals: Vec<RxLocal> = tokens.iter().map(|t| RxLocal::new(&t.rx)).collect();
        let with = Template::build(raw, &tokens, &mut locals, Some(&pf), &mut Vec::new()).unwrap();
        let without = template(raw, &tokens).unwrap();
        assert_eq!(with.segs, without.segs);
        assert_eq!(with.toks, without.toks);
    }
}
