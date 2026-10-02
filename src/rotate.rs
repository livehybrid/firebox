//! Identity rotation: a fresh identity for every pass over the sample.
//!
//! Stoker's pack builder can replace an identifier with a stable stand-in, which
//! keeps a replayed journey joinable. But a stable stand-in replays the SAME
//! user for ever: a three-event sample of login, register, logout yields
//! thousands of logins by one user, so anything that counts or groups by user is
//! meaningless. Rotation fixes that. Each pass over the sample mints a new
//! identity, and within the pass the login and the logout still belong to it.
//!
//! The identity is a **counter, not a hash**, which is the important design
//! choice. The identities are synthetic, so there is nothing to conceal, and a
//! counter is collision-free by construction where a hash is not: at the scale
//! this runs (tens of thousands of events per second per worker, dozens of
//! workers) a hash would need 14 to 19 digits to avoid an expected merge, where
//! the counter needs 7 to 10. It also means firebox needs no crypto dependency.
//!
//! ```text
//! ident = (pass x S + slot) x D_F + k_F(t)
//! ```
//!
//! `pass` is how many times the cursor has wrapped the sample, `S` the worker
//! count, `slot` this worker's index, `D_F` the number of distinct values in the
//! sample that share the format class `F`, and `k_F(t)` the position of this
//! value in that class. Read right to left that is a mixed-radix number, so the
//! map is **injective**: two workers differ in the `slot` digit and therefore can
//! never mint the same identity, which is the property that would otherwise
//! embarrass us at a customer running 58 slots. Within a class `render` is
//! injective too, and across classes it is class-stable (see [`crate::format`]),
//! so two identities never render to the same string.
//!
//! The tables are derived from the sample itself at load, identically by every
//! worker and by the Python engine, so nothing has to be shipped or agreed at
//! run time.
//!
//! # Two scopes
//!
//! A pass counter gives the most identities, but `ordinal` and `lines` are
//! private to one stanza, so two sourcetypes are never at the same pass and an
//! identity cannot be joined across them. The only thing two independently
//! running streams agree on without coordination is the clock, so the second
//! scope counts **time windows** instead of passes, and derives the identity
//! from the value rather than from a position in a pack-local table:
//!
//! ```text
//! ident = mix(stand-in, window) mod space(F)      where window = epoch / period
//! ```
//!
//! Every pack, every run and every worker slot computes the same thing from the
//! same stand-in, so a client id joins across sourcetypes. Three things are
//! worth knowing about the trade:
//!
//! * It hashes the **stand-in**, not the original. The pack already holds
//!   stand-ins (mode 1 rewrote them when it was written), so there is no key to
//!   distribute, and a worker too old to know `rotate` emits the stable stand-in
//!   rather than leaking an identifier.
//! * A hash collides where a counter cannot, at exactly the birthday rate the
//!   builder already warns about for mode 1. Ticking alignment therefore buys
//!   joins at the cost of the collision guarantee, and the widen option is the
//!   answer to both.
//! * Cardinality is `table x duration / period`, not `table x passes`, so the
//!   period is the dial between "realistic user count" and "joinable".

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

use crate::format::{self, Format};

/// Parsed from `token.N.replacement`: how to render the rotated identity.
///
/// `keep` renders in the format of the value being replaced, which is what the
/// builder writes, so widening the stable stand-in widens the rotation with it
/// and one width is chosen in one place.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Policy {
    Keep,
    Fixed(Format),
}

impl Policy {
    /// Parse `keep`, `digits(N)`, `hex(N)` or `guid`. Anything else is an error
    /// naming what was given, so a hand-authored pack fails loudly at load.
    pub fn parse(text: &str) -> Result<Policy, String> {
        let t = text.trim();
        if t.is_empty() || t.eq_ignore_ascii_case("keep") || t.eq_ignore_ascii_case("same") {
            return Ok(Policy::Keep);
        }
        if t.eq_ignore_ascii_case("guid") {
            return Ok(Policy::Fixed(Format::Guid));
        }
        let (name, rest) = match t.split_once('(') {
            Some((name, rest)) => (name.trim(), rest.trim_end_matches(')').trim()),
            None => return Err(format!("rotate replacement {text:?} must be keep, digits(N), hex(N) or guid")),
        };
        let width: usize = rest.parse().map_err(|_| format!("rotate replacement {text:?} has a non-numeric width"))?;
        if width == 0 || width > 48 {
            return Err(format!("rotate replacement {text:?} width must be 1 to 48"));
        }
        match name.to_ascii_lowercase().as_str() {
            "digits" => Ok(Policy::Fixed(Format::Digits(width))),
            "hex" => Ok(Policy::Fixed(Format::Hex(width))),
            other => Err(format!("rotate replacement {text:?} has unknown shape {other:?}; use digits, hex or guid")),
        }
    }

    /// The format this policy renders `text` in: its own shape under `keep`,
    /// or the fixed shape under a widening policy. The rotation tables must be
    /// keyed by this, never by `infer(text)`.
    pub fn format_for(&self, text: &str) -> Format {
        match self {
            Policy::Keep => format::infer(text),
            Policy::Fixed(f) => f.clone(),
        }
    }
}

/// What counts as "a new identity": passes over the sample, or wall-clock
/// windows that every stanza in the estate agrees on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scope {
    Pass,
    Window { period: u64 },
}

impl Scope {
    /// Parse the per-stanza `rotate.scope` and `rotate.period` settings.
    pub fn parse(scope: &str, period: Option<u64>) -> Result<Scope, String> {
        let s = scope.trim();
        if s.is_empty() || s.eq_ignore_ascii_case("pass") {
            return Ok(Scope::Pass);
        }
        if s.eq_ignore_ascii_case("window") || s.eq_ignore_ascii_case("aligned") {
            let period = period.unwrap_or(60);
            if period == 0 {
                return Err("rotate.period must be at least 1 second".to_string());
            }
            return Ok(Scope::Window { period });
        }
        Err(format!("rotate.scope {scope:?} must be pass or window"))
    }

    pub fn is_aligned(&self) -> bool {
        matches!(self, Scope::Window { .. })
    }
}

/// Where an event sits: its ordinal within the stanza, and when it happened.
/// `Pass` scope reads the ordinal, `Window` scope reads the time, and the emit
/// loop knows both, so neither scope needs a special path through the engine.
#[derive(Clone, Copy, Debug)]
pub struct Position {
    pub ordinal: u64,
    pub epoch: i64,
}

impl Position {
    pub fn new(ordinal: u64, epoch: i64) -> Position {
        Position { ordinal, epoch }
    }
}

/// The window an event falls in. Anchored on the Unix epoch, which needs no
/// agreement beyond the period itself, and clamped so a pre-1970 replay
/// timestamp cannot wrap into a huge window.
pub fn window_index(epoch: i64, period: u64) -> u64 {
    (epoch.max(0) as u64) / period.max(1)
}

/// A format class, as a hashable key. `mixed` carries its template signature,
/// because two mixed values of different shapes are different classes.
fn class_key(format: &Format) -> String {
    match format {
        Format::Digits(w) => format!("d{w}"),
        Format::Hex(w) => format!("h{w}"),
        Format::Guid => "g".to_string(),
        Format::None => "n".to_string(),
        Format::Mixed(slots) => {
            let mut key = String::from("m");
            for slot in slots {
                match slot {
                    format::Slot::Verbatim(ch) => {
                        key.push('v');
                        key.push(*ch);
                    }
                    format::Slot::Choice(radix, alphabet) => {
                        key.push('c');
                        key.push_str(&radix.to_string());
                        // the alphabet's first byte distinguishes upper from lower
                        key.push(alphabet[0] as char);
                    }
                }
            }
            key
        }
    }
}

/// The distinct sample values of one format class, in file order.
#[derive(Default, Debug)]
pub struct Tables {
    by_class: HashMap<String, HashMap<String, usize>>,
    sizes: HashMap<String, u64>,
}

impl Tables {
    /// Record a value seen in the sample, under the format its token will use.
    ///
    /// This must be the token's own [`Policy`], not `infer(text)`: a widening
    /// policy puts the value in a different class from its own shape, and a
    /// table built under the wrong class never matches, so the rotation
    /// silently leaves every event alone.
    pub fn observe_policy(&mut self, policy: &Policy, text: &str) {
        self.observe(&policy.format_for(text), text);
    }

    /// Record a value under an explicit format. Order of calls defines `k`, so
    /// every implementation must walk the sample lines in file order and each
    /// token's matches left to right.
    pub fn observe(&mut self, format: &Format, text: &str) {
        if matches!(format, Format::None) {
            return;
        }
        let key = class_key(format);
        let table = self.by_class.entry(key.clone()).or_default();
        let next = table.len();
        table.entry(text.to_string()).or_insert(next);
        self.sizes.insert(key, table.len() as u64);
    }

    /// `(k, D)` for a value, or None when it was not in the sample (only
    /// possible if an earlier token rewrote the span, which token order
    /// prevents; the caller counts it and leaves the text alone).
    pub fn lookup(&self, format: &Format, text: &str) -> Option<(u64, u64)> {
        let key = class_key(format);
        let table = self.by_class.get(&key)?;
        let k = *table.get(text)?;
        Some((k as u64, *self.sizes.get(&key).unwrap_or(&1)))
    }

    /// Slot-passes available before the tightest class wraps and identities
    /// start repeating. The builder writes this into pack.yaml so a run can be
    /// checked against its planned volume before it starts.
    pub fn capacity(&self) -> u128 {
        let mut smallest = u128::MAX;
        for (key, size) in &self.sizes {
            let format = match key.chars().next() {
                Some('d') => Format::Digits(key[1..].parse().unwrap_or(1)),
                Some('h') => Format::Hex(key[1..].parse().unwrap_or(1)),
                Some('g') => Format::Guid,
                _ => continue, // mixed: its space is recovered from a member below
            };
            smallest = smallest.min(format::space(&format) / u128::from(*size).max(1));
        }
        for (key, size) in &self.sizes {
            if !key.starts_with('m') {
                continue;
            }
            if let Some(text) = self.by_class.get(key).and_then(|t| t.keys().next()) {
                let space = format::space(&format::infer(text));
                smallest = smallest.min(space / u128::from(*size).max(1));
            }
        }
        if smallest == u128::MAX {
            0
        } else {
            smallest
        }
    }

    pub fn is_empty(&self) -> bool {
        self.by_class.is_empty()
    }

    pub fn classes(&self) -> usize {
        self.by_class.len()
    }
}

/// Per-stanza rotation state, shared by every `rotate` token in the stanza so
/// that a value matched by two different patterns rotates as one identity.
#[derive(Debug)]
pub struct State {
    /// Event ordinals handed out so far. Continuous across intervals, which is
    /// what removes the orphaned partial pass that restarting at line 0 would
    /// leave at the end of every interval.
    cursor: AtomicU64,
    tables: OnceLock<Tables>,
    /// Sample lines, so `pass = ordinal / lines`.
    lines: u64,
    workers: u64,
    slot: u64,
    scope: Scope,
    wraps: AtomicU64,
    unknown: AtomicU64,
}

impl State {
    pub fn new(lines: usize, workers: u64, slot: u64, base: u64, scope: Scope) -> State {
        State {
            cursor: AtomicU64::new(base),
            tables: OnceLock::new(),
            lines: lines.max(1) as u64,
            workers: workers.max(1),
            slot,
            scope,
            wraps: AtomicU64::new(0),
            unknown: AtomicU64::new(0),
        }
    }

    pub fn scope(&self) -> Scope {
        self.scope
    }

    /// Install the tables derived from the sample. Called once, before any event.
    pub fn set_tables(&self, tables: Tables) {
        let _ = self.tables.set(tables);
    }

    pub fn tables(&self) -> Option<&Tables> {
        self.tables.get()
    }

    /// Reserve `count` consecutive ordinals and return the first.
    ///
    /// One atomic add per timer fire, so the blocks are disjoint and increasing
    /// whichever thread later renders them: an ordinal belongs to exactly one
    /// event, and two events share a pass exactly when they fall in the same
    /// block of `lines` consecutive ordinals, which is the definition of a pass.
    pub fn reserve(&self, count: u64) -> u64 {
        self.cursor.fetch_add(count, Ordering::Relaxed)
    }

    /// The ordinals handed out so far, for the state file and the heartbeat.
    pub fn cursor(&self) -> u64 {
        self.cursor.load(Ordering::Relaxed)
    }

    pub fn wraps(&self) -> u64 {
        self.wraps.load(Ordering::Relaxed)
    }

    pub fn unknown(&self) -> u64 {
        self.unknown.load(Ordering::Relaxed)
    }

    pub fn line_for(&self, ordinal: u64) -> usize {
        (ordinal % self.lines) as usize
    }

    /// The rotated value for `text` at this event's position, or None to leave
    /// the text alone.
    pub fn render(&self, policy: &Policy, text: &str, at: Position) -> Option<String> {
        // An empty match has nothing to rotate. The guard matters because a
        // widening policy would otherwise mint an identity for it, where the
        // reference leaves it alone: a silent cross-engine divergence.
        if text.is_empty() {
            return None;
        }
        let format = policy.format_for(text);
        if matches!(format, Format::None) {
            return None;
        }
        let space = format::space(&format);
        let x = match self.scope {
            Scope::Pass => self.pass_ident(&format, text, at.ordinal, space)?,
            Scope::Window { period } => {
                // No table lookup: the identity is a function of the stand-in
                // and the window alone, which is exactly what lets two packs
                // that share neither a table nor a line count agree.
                aligned_ident(text, window_index(at.epoch, period), space)
            }
        };
        format::render(&format, x, format::case_class(text))
    }

    /// The pass counter: `(pass x S + slot) x D + k`, a mixed-radix number, so
    /// the map is injective and no two passes and no two slots can ever mint
    /// the same identity.
    fn pass_ident(&self, format: &Format, text: &str, ordinal: u64, space: u128) -> Option<u128> {
        let tables = self.tables.get()?;
        let (k, d) = match tables.lookup(format, text) {
            Some(found) => found,
            None => {
                self.unknown.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };
        let pass = ordinal / self.lines;
        let ident = u128::from(pass)
            .saturating_mul(u128::from(self.workers))
            .saturating_add(u128::from(self.slot))
            .saturating_mul(u128::from(d.max(1)))
            .saturating_add(u128::from(k));
        if ident >= space && self.wraps.fetch_add(1, Ordering::Relaxed) == 0 {
            log::error!(
                "rotate: the identity space of {format} is exhausted after {pass} passes; \
                 identities now repeat. Widen this field."
            );
        }
        Some(match format {
            // A GUID that counted upwards would be obviously synthetic, so the
            // high bits carry a hash of the value. The low bits still carry the
            // identity, so uniqueness is unaffected.
            Format::Guid => (u128::from(fnv1a64(text)) << 64) | (ident & u128::from(u64::MAX)),
            _ => ident,
        })
    }
}

/// The aligned identity: a pure function of the stand-in and the window.
///
/// Two mixes give 128 bits, so a GUID gets a full-width value and a narrow
/// format takes the modulus. The Python engine implements the same arithmetic
/// under a 64-bit mask; `fixtures/format_vectors.json` pins the results.
pub fn aligned_ident(text: &str, window: u64, space: u128) -> u128 {
    let seeded = fnv1a64(text) ^ window.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    let lo = mix64(seeded);
    let hi = mix64(lo ^ 0xa5a5_a5a5_a5a5_a5a5);
    let x = (u128::from(hi) << 64) | u128::from(lo);
    x % space.max(1)
}

/// splitmix64's finaliser: cheap, dependency-free, and avalanches well enough
/// that consecutive windows do not produce visibly consecutive identities.
pub fn mix64(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// FNV-1a over the UTF-8 bytes. Only used to make a rotated GUID look random in
/// its high bits; it is not a security function and the Python engine must
/// implement the same 64-bit arithmetic.
pub fn fnv1a64(text: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in text.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}

/// Round an ordinal up to the start of the next pass, for a restart: the saved
/// cursor already covers part of a pass, and resuming mid-pass would re-mint an
/// identity already sent to Splunk.
pub fn round_up_to_pass(ordinal: u64, lines: u64) -> u64 {
    let lines = lines.max(1);
    ordinal.div_ceil(lines) * lines
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tables_for(values: &[&str]) -> Tables {
        tables_under(&Policy::Keep, values)
    }

    fn tables_under(policy: &Policy, values: &[&str]) -> Tables {
        let mut tables = Tables::default();
        for value in values {
            tables.observe_policy(policy, value);
        }
        tables
    }

    fn state(lines: usize, workers: u64, slot: u64, values: &[&str]) -> State {
        scoped(lines, workers, slot, values, Scope::Pass)
    }

    fn scoped(lines: usize, workers: u64, slot: u64, values: &[&str], scope: Scope) -> State {
        under(lines, workers, slot, values, scope, &Policy::Keep)
    }

    fn under(lines: usize, workers: u64, slot: u64, values: &[&str], scope: Scope, policy: &Policy) -> State {
        let state = State::new(lines, workers, slot, 0, scope);
        state.set_tables(tables_under(policy, values));
        state
    }

    /// Pass scope ignores the time, so the tests about passes pass 0 for it.
    fn at(ordinal: u64) -> Position {
        Position::new(ordinal, 0)
    }

    #[test]
    fn policies_parse_and_bad_ones_say_why() {
        assert_eq!(Policy::parse("keep").unwrap(), Policy::Keep);
        assert_eq!(Policy::parse("").unwrap(), Policy::Keep);
        assert_eq!(Policy::parse("guid").unwrap(), Policy::Fixed(Format::Guid));
        assert_eq!(Policy::parse("digits(15)").unwrap(), Policy::Fixed(Format::Digits(15)));
        assert_eq!(Policy::parse("hex(16)").unwrap(), Policy::Fixed(Format::Hex(16)));
        for bad in ["alnum(12)", "digits(x)", "digits(0)", "digits(49)", "nonsense"] {
            let err = Policy::parse(bad).unwrap_err();
            assert!(err.contains(bad), "{bad}: {err}");
        }
    }

    #[test]
    fn a_pass_shares_one_identity_and_the_next_pass_differs() {
        // The customer's sample: lines 0 and 2 are the same user.
        let s = state(3, 1, 0, &["123", "456"]);
        let first = s.render(&Policy::Keep, "123", at(0)).unwrap();
        let third = s.render(&Policy::Keep, "123", at(2)).unwrap();
        let other = s.render(&Policy::Keep, "456", at(1)).unwrap();
        assert_eq!(first, third, "login and logout must share the identity");
        assert_ne!(first, other, "a different user must stay different");

        // Pass 1 (ordinals 3..5) is a NEW user, still consistent within itself.
        let next_first = s.render(&Policy::Keep, "123", at(3)).unwrap();
        let next_third = s.render(&Policy::Keep, "123", at(5)).unwrap();
        assert_eq!(next_first, next_third);
        assert_ne!(first, next_first, "each replay must be a new user");
    }

    #[test]
    fn the_format_is_kept() {
        let s = state(2, 1, 0, &["123456", "ada.smith"]);
        let digits = s.render(&Policy::Keep, "123456", at(0)).unwrap();
        assert_eq!(digits.len(), 6);
        assert!(digits.chars().all(|c| c.is_ascii_digit()));
        let mixed = s.render(&Policy::Keep, "ada.smith", at(0)).unwrap();
        assert_eq!(mixed.len(), 9);
        assert_eq!(mixed.as_bytes()[3], b'.', "the separator stays put");
    }

    #[test]
    fn two_slots_never_mint_the_same_identity() {
        // The property that matters at the customer's 58 slots. Every slot walks
        // the same ordinals, and no identity may appear in two of them.
        const SLOTS: u64 = 4;
        let wide = Policy::Fixed(Format::Digits(12));
        let mut seen: HashMap<String, u64> = HashMap::new();
        for slot in 0..SLOTS {
            let s = under(3, SLOTS, slot, &["123", "456"], Scope::Pass, &wide);
            for ordinal in 0..600 {
                for value in ["123", "456"] {
                    let out = s.render(&wide, value, at(ordinal)).unwrap();
                    if let Some(other) = seen.insert(out.clone(), slot) {
                        assert_eq!(other, slot, "identity {out} minted by two slots");
                    }
                }
            }
        }
        assert!(seen.len() > 1000);
    }

    #[test]
    fn identities_are_distinct_across_passes_within_a_slot() {
        let wide = Policy::Fixed(Format::Digits(12));
        let s = under(3, 1, 0, &["123", "456"], Scope::Pass, &wide);
        let mut seen = std::collections::HashSet::new();
        for pass in 0..500u64 {
            let out = s.render(&wide, "123", at(pass * 3)).unwrap();
            assert!(seen.insert(out), "pass {pass} reused an identity");
        }
    }

    #[test]
    fn two_values_of_different_classes_cannot_collide() {
        // Class stability is what guarantees this; check it holds through the
        // identity arithmetic too.
        let s = state(2, 1, 0, &["123456", "abcdef"]);
        for ordinal in 0..200 {
            let a = s.render(&Policy::Keep, "123456", at(ordinal)).unwrap();
            let b = s.render(&Policy::Keep, "abcdef", at(ordinal)).unwrap();
            assert_ne!(a, b);
            assert!(a.chars().all(|c| c.is_ascii_digit()));
            assert!(b.chars().any(|c| c.is_ascii_alphabetic()));
        }
    }

    #[test]
    fn a_value_absent_from_the_sample_is_left_alone_and_counted() {
        let s = state(2, 1, 0, &["123"]);
        assert!(s.render(&Policy::Keep, "999", at(0)).is_none());
        assert_eq!(s.unknown(), 1);
    }

    #[test]
    fn nothing_to_vary_is_left_alone() {
        let s = state(1, 1, 0, &["---"]);
        assert!(s.render(&Policy::Keep, "---", at(0)).is_none());
    }

    #[test]
    fn exhausting_the_space_wraps_and_is_counted_once() {
        // A 1-digit field holds 9 identities, so pass 9 must wrap rather than
        // abort a 58-worker run over a cardinality defect.
        let s = state(1, 1, 0, &["5"]);
        let first = s.render(&Policy::Keep, "5", at(0)).unwrap();
        for ordinal in 1..9 {
            assert!(s.render(&Policy::Keep, "5", at(ordinal)).is_some());
        }
        assert_eq!(s.wraps(), 0, "nothing should wrap inside the space");
        let wrapped = s.render(&Policy::Keep, "5", at(9)).unwrap();
        assert_eq!(wrapped, first, "the modulus brings it back round");
        assert_eq!(s.wraps(), 1);
        s.render(&Policy::Keep, "5", at(10)).unwrap();
        assert_eq!(s.wraps(), 2, "wraps keep counting, the log happens once");
    }

    #[test]
    fn reserving_ordinals_hands_out_disjoint_blocks() {
        let s = state(3, 1, 0, &["1"]);
        assert_eq!(s.reserve(10), 0);
        assert_eq!(s.reserve(10), 10);
        assert_eq!(s.cursor(), 20);
        assert_eq!(s.line_for(0), 0);
        assert_eq!(s.line_for(4), 1);
        assert_eq!(s.line_for(5), 2);
    }

    #[test]
    fn a_restart_resumes_on_a_pass_boundary() {
        // Resuming mid-pass would re-mint an identity already in Splunk.
        assert_eq!(round_up_to_pass(0, 3), 0);
        assert_eq!(round_up_to_pass(1, 3), 3);
        assert_eq!(round_up_to_pass(3, 3), 3);
        assert_eq!(round_up_to_pass(4, 3), 6);
        let s = State::new(3, 1, 0, round_up_to_pass(4, 3), Scope::Pass);
        assert_eq!(s.reserve(1), 6);
    }

    #[test]
    fn capacity_is_the_tightest_class() {
        // 2 distinct 3-digit values in a 900 space -> 450 slot-passes.
        let tables = tables_for(&["123", "456"]);
        assert_eq!(tables.capacity(), 450);
        // Adding a 1-digit value (9 values, 1 entry) makes that the tightest.
        let tables = tables_for(&["123", "456", "7"]);
        assert_eq!(tables.capacity(), 9);
        assert_eq!(tables_for(&[]).capacity(), 0);
    }

    #[test]
    fn a_guid_rotates_but_still_looks_random() {
        let one = "3f2b1c9e-8a7d-4e21-9b3c-1d2e3f4a5b6c";
        let s = state(1, 1, 0, &[one]);
        let a = s.render(&Policy::Keep, one, at(0)).unwrap();
        let b = s.render(&Policy::Keep, one, at(1)).unwrap();
        assert_ne!(a, b, "a new pass is a new GUID");
        assert_eq!(a.len(), 36);
        assert_eq!(&a[14..15], "4");
        // the leading bytes come from the value, not the counter, so two
        // consecutive passes do not look consecutive
        assert_eq!(a[..8], b[..8]);
        assert_eq!(format::infer(&a), Format::Guid);
    }

    #[test]
    fn fnv_matches_the_reference_vector() {
        // The canonical FNV-1a-64 test vector, so the Python engine can agree.
        assert_eq!(fnv1a64(""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a64("a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a64("foobar"), 0x8594_4171_f739_67e8);
    }

    #[test]
    fn the_tables_number_values_in_the_order_they_are_seen() {
        let tables = tables_for(&["456", "123", "456", "789"]);
        let digits = format::infer("456");
        assert_eq!(tables.lookup(&digits, "456"), Some((0, 3)));
        assert_eq!(tables.lookup(&digits, "123"), Some((1, 3)));
        assert_eq!(tables.lookup(&digits, "789"), Some((2, 3)));
        assert_eq!(tables.lookup(&digits, "000"), None);
        assert_eq!(tables.classes(), 1);
    }

    #[test]
    fn a_widening_policy_still_finds_its_values() {
        // Regression: the tables were built with infer(text) while the lookup
        // used the policy's format, so a widening token matched nothing and
        // every event was emitted unrotated. Silent, and exactly the case the
        // collision warning pushes operators towards.
        let wide = Policy::Fixed(Format::Digits(15));
        let s = under(2, 1, 0, &["123", "456"], Scope::Pass, &wide);
        let out = s.render(&wide, "123", at(0)).expect("widened value must rotate");
        assert_eq!(out.len(), 15);
        assert_eq!(s.unknown(), 0);
    }

    // ---- window scope: the cross-sourcetype tickbox -----------------------

    #[test]
    fn scopes_parse_and_bad_ones_say_why() {
        assert_eq!(Scope::parse("pass", None).unwrap(), Scope::Pass);
        assert_eq!(Scope::parse("", None).unwrap(), Scope::Pass);
        assert_eq!(Scope::parse("window", None).unwrap(), Scope::Window { period: 60 });
        assert_eq!(Scope::parse("window", Some(5)).unwrap(), Scope::Window { period: 5 });
        assert!(Scope::parse("window", Some(0)).is_err());
        assert!(Scope::parse("hourly", None).unwrap_err().contains("hourly"));
        assert!(Scope::parse("window", None).unwrap().is_aligned());
        assert!(!Scope::parse("pass", None).unwrap().is_aligned());
    }

    #[test]
    fn two_sourcetypes_that_share_nothing_still_agree() {
        // This is the whole point of the option. Two packs built separately:
        // different sample sizes, different value sets, different worker
        // counts, different slots, different ordinals. The only thing they have
        // in common is the stand-in and the clock, and that must be enough.
        let web = scoped(3, 4, 2, &["aa1", "bb2", "cc3"], Scope::Window { period: 60 });
        let auth = scoped(17, 1, 0, &["zz9"], Scope::Window { period: 60 });
        let epoch = 1_760_000_040; // on a 60s boundary, so +59 is the same window
        let from_web = web.render(&Policy::Keep, "884412", Position::new(5, epoch)).unwrap();
        let from_auth = auth.render(&Policy::Keep, "884412", Position::new(9001, epoch + 59)).unwrap();
        assert_eq!(from_web, from_auth, "a client id must join across sourcetypes");
    }

    #[test]
    fn the_window_still_rotates() {
        // Aligned must not mean static: the next window is a new identity.
        let s = scoped(3, 1, 0, &["123456"], Scope::Window { period: 60 });
        // 1_000_000_020 is 60 x 16_666_667, so the window runs to +80.
        let first = s.render(&Policy::Keep, "123456", Position::new(0, 1_000_000_020)).unwrap();
        let same = s.render(&Policy::Keep, "123456", Position::new(999, 1_000_000_079)).unwrap();
        let next = s.render(&Policy::Keep, "123456", Position::new(1, 1_000_000_080)).unwrap();
        assert_eq!(first, same, "one window is one identity");
        assert_ne!(first, next, "the next window is a new identity");
        assert_eq!(first.len(), 6);
    }

    #[test]
    fn window_scope_ignores_the_worker_slot() {
        // Under pass scope the slot digit is what keeps workers disjoint. Under
        // window scope it must be absent, or the same client id would differ
        // between workers and the join would break inside a single run.
        let epoch = 1_700_000_000;
        let mut seen = std::collections::HashSet::new();
        for slot in 0..8u64 {
            let s = scoped(3, 8, slot, &["123456"], Scope::Window { period: 60 });
            seen.insert(s.render(&Policy::Keep, "123456", Position::new(slot, epoch)).unwrap());
        }
        assert_eq!(seen.len(), 1, "every slot must mint the same identity: {seen:?}");
    }

    #[test]
    fn window_scope_needs_no_table() {
        // A stand-in that reached the event through another token, or a widened
        // one, is still rotated: there is no table to be absent from.
        let s = scoped(1, 1, 0, &[], Scope::Window { period: 60 });
        assert!(s.render(&Policy::Keep, "778899", Position::new(0, 1_700_000_000)).is_some());
        assert_eq!(s.unknown(), 0);
        // Pass scope, by contrast, leaves it alone and counts it.
        let p = scoped(1, 1, 0, &[], Scope::Pass);
        assert!(p.render(&Policy::Keep, "778899", at(0)).is_none());
        assert_eq!(p.unknown(), 1);
    }

    #[test]
    fn window_scope_keeps_the_format_and_the_class() {
        let s = scoped(1, 1, 0, &[], Scope::Window { period: 60 });
        for (text, want) in [
            ("123456", format::Format::Digits(6)),
            ("a1b2c3", format::Format::Hex(6)),
            ("ada.smith", format::infer("ada.smith")),
            ("3f2b1c9e-8a7d-4e21-9b3c-1d2e3f4a5b6c", format::Format::Guid),
        ] {
            for window in 0..40i64 {
                let out = s.render(&Policy::Keep, text, Position::new(0, window * 60)).unwrap();
                assert_eq!(out.len(), text.len(), "{text}");
                assert_eq!(format::infer(&out), want, "{text} -> {out}");
            }
        }
    }

    #[test]
    fn window_scope_spreads_over_the_space() {
        // A hash can collide where the counter cannot, which is the documented
        // cost of the option. What must not happen is clustering: 2000 windows
        // of a 6-digit field should give close to 2000 distinct identities, at
        // roughly the birthday rate rather than some degenerate cycle.
        let s = scoped(1, 1, 0, &[], Scope::Window { period: 1 });
        let mut seen = std::collections::HashSet::new();
        for window in 0..2000i64 {
            seen.insert(s.render(&Policy::Keep, "123456", Position::new(0, window)).unwrap());
        }
        assert!(seen.len() > 1960, "only {} distinct of 2000", seen.len());
    }

    #[test]
    fn windows_are_anchored_on_the_epoch() {
        assert_eq!(window_index(0, 60), 0);
        assert_eq!(window_index(59, 60), 0);
        assert_eq!(window_index(60, 60), 1);
        assert_eq!(window_index(1_760_000_000, 60), 1_760_000_000 / 60);
        // A pre-epoch replay timestamp must not wrap into a huge window.
        assert_eq!(window_index(-5, 60), 0);
    }

    #[test]
    fn the_mixer_matches_the_reference_vector() {
        // splitmix64's finaliser, so the Python engine can agree.
        assert_eq!(mix64(0), 0);
        assert_eq!(mix64(1), 0x5692_161d_100b_05e5);
        assert_eq!(mix64(0xdead_beef), 0x4e06_2702_ec92_9eea);
    }

    // ---- the cross-engine contract for aligned rotation -------------------

    /// The shared contract file, written by Stoker's reference implementation.
    fn vectors() -> serde_json::Value {
        let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("fixtures/format_vectors.json");
        if !path.exists() {
            path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            path.push("../fixtures/format_vectors.json");
        }
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
        serde_json::from_str(&text).expect("fixture is not valid JSON")
    }

    fn policy_of(widen: &serde_json::Value) -> Policy {
        if widen.is_null() {
            return Policy::Keep;
        }
        let length = widen.get("length").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
        match widen["shape"].as_str().unwrap() {
            "digits" => Policy::Fixed(Format::Digits(length)),
            "hex" => Policy::Fixed(Format::Hex(length)),
            "guid" => Policy::Fixed(Format::Guid),
            other => panic!("unknown widen shape {other}"),
        }
    }

    #[test]
    fn aligned_rotation_matches_the_reference_vectors() {
        // The identity is a function of the stand-in and the clock, so a worker
        // that disagrees with the control plane by one bit stops joining across
        // sourcetypes, silently. Hence byte-for-byte vectors rather than
        // property tests alone.
        let v = vectors();
        let rows = v["window_render"].as_array().expect("window_render missing");
        assert!(rows.len() > 500, "only {} vectors", rows.len());
        let mut checked = 0;
        for entry in rows {
            let text = entry["text"].as_str().unwrap();
            let window: u64 = entry["window"].as_str().unwrap().parse().unwrap();
            let policy = policy_of(&entry["widen"]);
            let state = State::new(1, 1, 0, 0, Scope::Window { period: 1 });
            // period 1 so the epoch IS the window index.
            let got = state.render(&policy, text, Position::new(0, window as i64));
            match entry["out"].as_str() {
                Some(want) => assert_eq!(got.as_deref(), Some(want), "{entry}"),
                None => assert_eq!(got, None, "{entry}"),
            }
            checked += 1;
        }
        assert_eq!(checked, rows.len());
    }

    #[test]
    fn window_indexing_matches_the_reference_vectors() {
        for entry in vectors()["window_index"].as_array().unwrap() {
            let epoch: i64 = entry["epoch"].as_str().unwrap().parse().unwrap();
            let period: u64 = entry["period"].as_str().unwrap().parse().unwrap();
            let want: u64 = entry["window"].as_str().unwrap().parse().unwrap();
            assert_eq!(window_index(epoch, period), want, "{entry}");
        }
    }

    #[test]
    fn the_mixers_match_the_reference_vectors() {
        let v = vectors();
        for entry in v["mixer"]["fnv1a64"].as_array().unwrap() {
            let want: u64 = entry["out"].as_str().unwrap().parse().unwrap();
            assert_eq!(fnv1a64(entry["text"].as_str().unwrap()), want, "{entry}");
        }
        for entry in v["mixer"]["mix64"].as_array().unwrap() {
            let x: u64 = entry["x"].as_str().unwrap().parse().unwrap();
            let want: u64 = entry["out"].as_str().unwrap().parse().unwrap();
            assert_eq!(mix64(x), want, "{entry}");
        }
    }

    #[test]
    fn a_non_ascii_stand_in_assumes_nfc() {
        // render() emits ASCII, but a `keep` mixed format copies the original's
        // separators through, so a stand-in can hold non-ASCII. The engines
        // hash bytes and do not normalise, so the invariant is that the builder
        // writes NFC into the pack; the vector above pins one such value.
        let v = vectors();
        let row = v["window_render"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| !e["text"].as_str().unwrap().is_ascii())
            .expect("the contract must pin a non-ASCII value");
        let text = row["text"].as_str().unwrap();
        assert_eq!(text.chars().count(), 6, "the fixture must be composed (NFC)");
        let state = State::new(1, 1, 0, 0, Scope::Window { period: 1 });
        let window: u64 = row["window"].as_str().unwrap().parse().unwrap();
        assert_eq!(
            state
                .render(&policy_of(&row["widen"]), text, Position::new(0, window as i64))
                .as_deref(),
            row["out"].as_str()
        );
    }
}
