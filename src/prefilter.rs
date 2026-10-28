//! Skipping the tokens that cannot match an event.
//!
//! Every token runs its regex over every event, in order. That is fine for a
//! pack with a handful of tokens and ruinous for one with thousands: a pack
//! builder pack varies each field it found with its own token, so a stanza
//! with 1,700 fields runs 1,700 regex searches per event even though each event
//! carries perhaps forty of those fields. A search that finds nothing still
//! costs a few hundred nanoseconds, so generation fell from ~175k events/s per
//! thread at forty tokens to ~1,700 at 1,700, with no single token slow.
//!
//! Most token patterns start with a fixed string (`\bstatus=(\d+)` cannot match
//! anywhere `status=` does not occur), so one Aho-Corasick pass over the event
//! finds every token that could match, and the rest are skipped.
//!
//! **Exact, not approximate.** A token is skipped only when none of its
//! pattern's possible prefixes occurs in the event *as it stands when that
//! token's turn comes*, which is a necessary condition for a match. The pass is
//! repeated after any token that changed the text, because a replacement can
//! introduce another token's prefix. That repeat is incremental: a new
//! occurrence must overlap the text a replacement wrote, so only those spans
//! (widened by the longest prefix) are scanned, and the set of tokens that
//! might match only grows within an event. A token left marked after its
//! prefix was overwritten just runs its search and finds nothing, as it did
//! before this existed. A token with no usable prefix
//! (a column pattern such as `^(?:[^,]*,){3}([^,]*)`, a lookaround that needs
//! the backtracking engine, a case-insensitive pattern that explodes into too
//! many variants) is never skipped. A skipped token is one whose search would
//! have found nothing, so it would have changed nothing and drawn no random
//! number: the output is byte-for-byte what it was.

use aho_corasick::{AhoCorasick, AhoCorasickBuilder, AhoCorasickKind, MatchKind};
use regex_syntax::hir::literal::{ExtractKind, Extractor};

use crate::pyre::Rx;
use crate::token::Token;

/// Below this many skippable tokens the pass costs more than it saves.
const MIN_GATED_TOKENS: usize = 8;
/// A token whose prefixes number more than this is left ungated: the set is
/// a case-folding or alternation blow-up, and it would match almost anywhere.
const MAX_PREFIXES_PER_TOKEN: usize = 64;
/// Shorter prefixes gate nothing worth gating.
const MIN_PREFIX_BYTES: usize = 2;

#[derive(Debug)]
pub struct TokenPrefilter {
    ac: AhoCorasick,
    /// For each automaton pattern, the tokens it gates.
    owners: Vec<Vec<u32>>,
    /// For each token, whether it is gated at all.
    gated: Vec<bool>,
    /// The longest prefix, in bytes: how far an occurrence can reach outside
    /// the text a replacement wrote and still overlap it.
    longest: usize,
}

impl TokenPrefilter {
    /// A prefilter for `tokens`, or None when too few of them can be gated for
    /// it to pay.
    pub fn build(tokens: &[Token]) -> Option<TokenPrefilter> {
        let mut literals: Vec<Vec<u8>> = Vec::new();
        let mut owners: Vec<Vec<u32>> = Vec::new();
        let mut gated = vec![false; tokens.len()];
        for (index, token) in tokens.iter().enumerate() {
            let Some(prefixes) = required_prefixes(&token.rx) else { continue };
            gated[index] = true;
            for prefix in prefixes {
                let at = match literals.iter().position(|l| *l == prefix) {
                    Some(at) => at,
                    None => {
                        literals.push(prefix);
                        owners.push(Vec::new());
                        literals.len() - 1
                    }
                };
                owners[at].push(index as u32);
            }
        }
        let count = gated.iter().filter(|g| **g).count();
        if count < MIN_GATED_TOKENS {
            return None;
        }
        // Standard semantics so overlapping occurrences are all reported: one
        // literal inside another (`id=` in `user_id=`) gates both tokens.
        // A DFA: thousands of short keys build a few MB at most, and it scans
        // several times faster than the NFA the builder would otherwise pick.
        let ac = match AhoCorasickBuilder::new()
            .match_kind(MatchKind::Standard)
            .kind(Some(AhoCorasickKind::DFA))
            .build(&literals)
        {
            Ok(ac) => ac,
            Err(e) => {
                log::debug!("token prefilter not built: {}", e);
                return None;
            }
        };
        log::debug!("token prefilter: {} of {} token(s) gated on {} prefix(es)", count, tokens.len(), literals.len());
        let longest = literals.iter().map(Vec::len).max().unwrap_or(0);
        Some(TokenPrefilter { ac, owners, gated, longest })
    }

    pub fn is_gated(&self, token: usize) -> bool {
        self.gated[token]
    }

    /// Mark in `present` every gated token whose prefix occurs in `text`.
    /// Ungated entries are left untouched; callers only read gated ones.
    pub fn scan(&self, text: &str, present: &mut Vec<bool>) {
        present.clear();
        present.resize(self.gated.len(), false);
        self.mark(text.as_bytes(), present);
    }

    /// Additionally mark the tokens whose prefix occurs overlapping any of
    /// `written` (byte ranges of `text` a replacement just wrote).
    pub fn rescan_written(&self, text: &str, written: &[(usize, usize)], present: &mut [bool]) {
        let bytes = text.as_bytes();
        let reach = self.longest.saturating_sub(1);
        let mut covered = 0usize; // windows are in order; never scan a byte twice
        for &(start, end) in written {
            let lo = start.saturating_sub(reach).max(covered);
            let hi = (end + reach).min(bytes.len());
            if lo < hi {
                self.mark(&bytes[lo..hi], present);
            }
            covered = covered.max(hi);
        }
    }

    fn mark(&self, haystack: &[u8], present: &mut [bool]) {
        for m in self.ac.find_overlapping_iter(haystack) {
            for &token in &self.owners[m.pattern().as_usize()] {
                present[token as usize] = true;
            }
        }
    }
}

/// The fixed strings one of which every match of `rx` must start with, or
/// None when there is no such finite, non-empty set.
fn required_prefixes(rx: &Rx) -> Option<Vec<Vec<u8>>> {
    let Rx::Std { re, .. } = rx else {
        return None; // the backtracking engine's syntax is not regex-syntax's
    };
    let hir = regex_syntax::ParserBuilder::new().octal(true).build().parse(re.as_str()).ok()?;
    let seq = Extractor::new().kind(ExtractKind::Prefix).extract(&hir);
    let literals = seq.literals()?;
    if literals.is_empty() || literals.len() > MAX_PREFIXES_PER_TOKEN {
        return None;
    }
    // An empty prefix means a match may begin anywhere; a one-byte prefix
    // (a digit class, say) is sound but occurs nearly everywhere, so it would
    // cost a scan and skip nothing.
    if literals.iter().any(|l| l.as_bytes().len() < MIN_PREFIX_BYTES) {
        return None;
    }
    Some(literals.iter().map(|l| l.as_bytes().to_vec()).collect())
}

/// Apply `tokens` to `event` in order, skipping those that cannot match.
///
/// `present` is scratch space reused across events. Returns nothing: each
/// token's own result is what it always was.
pub fn apply_tokens(
    tokens: &[Token],
    locals: &mut [crate::pyre::RxLocal],
    prefilter: Option<&TokenPrefilter>,
    present: &mut Vec<bool>,
    event: &mut String,
    ctx: &mut crate::token::EventCtx<'_>,
) {
    let Some(pf) = prefilter else {
        for (t, l) in tokens.iter().zip(locals.iter_mut()) {
            t.replace_local(event, ctx, l);
        }
        return;
    };
    pf.scan(event, present);
    let mut written: Vec<(usize, usize)> = Vec::new();
    for (index, (t, l)) in tokens.iter().zip(locals.iter_mut()).enumerate() {
        if pf.is_gated(index) && !present[index] {
            continue;
        }
        if !t.replace_local(event, ctx, l) {
            continue;
        }
        if written_spans(t, ctx, &mut written) {
            pf.rescan_written(event, &written, present);
        } else {
            // Where it wrote is not known (rotation writes a different value
            // per match): look again at the whole event.
            pf.scan(event, present);
        }
    }
}

/// The byte ranges of the rebuilt event that `token`'s replacement wrote, in
/// order, from the spans and value it left in `ctx`. False when they cannot be
/// derived that way.
fn written_spans(token: &Token, ctx: &crate::token::EventCtx<'_>, out: &mut Vec<(usize, usize)>) -> bool {
    if matches!(token.kind, crate::token::Kind::Rotate(_)) {
        return false;
    }
    out.clear();
    let len = ctx.value.len();
    let mut pos = 0usize; // end of the last replaced span, in the old text
    let mut delta: isize = 0; // new offset minus old offset
    for sp in ctx.spans.iter() {
        let (s, e) = sp.target();
        if s < pos {
            continue; // skipped by the splice too
        }
        let start = (s as isize + delta) as usize;
        out.push((start, start + len));
        delta += len as isize - (e - s) as isize;
        pos = e;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rx(p: &str) -> Rx {
        Rx::compile(p).unwrap()
    }

    #[test]
    fn a_key_value_pattern_needs_its_key() {
        let p = required_prefixes(&rx(r#"\bstatus=([^\s,;&"'\])]+)"#)).unwrap();
        assert_eq!(p, vec![b"status=".to_vec()]);
    }

    #[test]
    fn an_alternation_needs_one_of_its_branches() {
        let mut p = required_prefixes(&rx(r"\b(?:GET|POST)\b")).unwrap();
        p.sort();
        assert_eq!(p, vec![b"GET".to_vec(), b"POST".to_vec()]);
    }

    #[test]
    fn a_pattern_that_can_start_anywhere_is_never_gated() {
        assert!(required_prefixes(&rx(r"^(?:[^,\n]*,){3}([^,\n]*)")).is_none());
        assert!(required_prefixes(&rx(r"\d{4}-\d{2}-\d{2}")).is_none());
        assert!(required_prefixes(&rx(r"(\w+)@example")).is_none());
    }

    #[test]
    fn a_lookaround_is_never_gated() {
        assert!(required_prefixes(&rx(r"(?<=user=)\w+")).is_none());
    }

    use crate::conf::TokenSpec;
    use crate::rate::RateMaps;
    use crate::strftime::Ts;
    use crate::token::{EventCtx, Token};
    use rand::rngs::SmallRng;
    use rand::{Rng, RngCore, SeedableRng};
    use std::path::Path;

    fn tok(index: usize, token: &str, kind: &str, rep: &str) -> Token {
        let spec = TokenSpec {
            index,
            token: token.into(),
            replacement_type: kind.into(),
            replacement: rep.into(),
            ..Default::default()
        };
        Token::compile(&spec, Path::new("/"), Path::new("/tmp"), 0).unwrap()
    }

    /// The run the prefilter must reproduce exactly: every token, in order.
    fn render(tokens: &[Token], pf: Option<&TokenPrefilter>, event: &str, seed: u64) -> (String, u64) {
        let mut rng = SmallRng::seed_from_u64(seed);
        let maps = RateMaps::default();
        let now = Ts::from_epoch(1_800_000_000, 0).unwrap();
        let mut ctx = EventCtx::new(&mut rng, now, &maps);
        ctx.pivot = Some(now);
        ctx.et = Some(now);
        ctx.lt = Some(now);
        let mut locals: Vec<_> = tokens.iter().map(|t| crate::pyre::RxLocal::new(&t.rx)).collect();
        let mut present = Vec::new();
        let mut text = event.to_string();
        ctx.begin_event();
        apply_tokens(tokens, &mut locals, pf, &mut present, &mut text, &mut ctx);
        drop(ctx);
        (text, rng.next_u64())
    }

    #[test]
    fn the_output_is_exactly_what_every_token_in_turn_produces() {
        let mut tokens = Vec::new();
        // Ordinary key=value fields, as the pack builder writes them.
        for k in 0..60 {
            tokens.push(tok(tokens.len(), &format!(r#"\bf{:02}=([^\s,;&"'\])]+)"#, k), "random", "integer[1:99999]"));
        }
        // A replacement that WRITES a later token's key, so it must match.
        tokens.push(tok(tokens.len(), "##ADD##", "static", "f59=planted"));
        // One that completes a later key across the edge of what it wrote:
        // "f5" + "8=edge" makes "f58=edge".
        tokens.push(tok(tokens.len(), "##HALF##", "static", "8=edge"));
        // One that REMOVES a later token's key.
        tokens.push(tok(tokens.len(), r"\bf57=\S+", "static", "erased"));
        // A key inside a longer key: both tokens see it.
        tokens.push(tok(tokens.len(), r"\buser_id=(\w+)", "random", "integer[1:9]"));
        tokens.push(tok(tokens.len(), r"id=(\w+)", "random", "hex(4)"));
        // Ungated tokens: no usable prefix, always run.
        tokens.push(tok(tokens.len(), r"\b(\d{4})\b", "random", "integer[1000:9999]"));
        tokens.push(tok(tokens.len(), r"^(?:[^,\n]*,){2}([^,\n]*)", "static", "col2"));
        // The late keys the writers above interact with.
        for k in 57..60 {
            tokens.push(tok(tokens.len(), &format!(r#"\bf{:02}=([^\s,;&"'\])]+)"#, k), "static", "late"));
        }
        let pf = TokenPrefilter::build(&tokens).expect("enough gated tokens");

        let mut gen = SmallRng::seed_from_u64(11);
        let pieces = ["##ADD##", "f5##HALF##", "f57=x", "user_id=abc", "id=q", "2024", "a,b,c,d", "noise", "f5"];
        for case in 0..3000u64 {
            let mut parts = Vec::new();
            for _ in 0..gen.random_range(1..25) {
                if gen.random_bool(0.6) {
                    parts.push(format!("f{:02}=v{}", gen.random_range(0..60), gen.random_range(0..1000)));
                } else {
                    parts.push(pieces[gen.random_range(0..pieces.len())].to_string());
                }
            }
            let event = parts.join(if gen.random_bool(0.5) { " " } else { "," });
            let plain = render(&tokens, None, &event, case);
            let gated = render(&tokens, Some(&pf), &event, case);
            assert_eq!(plain, gated, "event {:?}", event);
        }
    }
}
