//! The format table shared with Stoker's pack builder, for identity rotation.
//!
//! Stoker's `rotate` replacement mints a NEW identity for every pass over a
//! sample, so a replayed journey belongs to a fresh user each time round while
//! staying consistent within the pass. The identity is a counter, and this
//! module turns that counter into a string **in the shape of the value it
//! replaces**: six digits in, six different digits out, so the customer's field
//! extractions, numeric comparisons and dashboards keep working.
//!
//! `server/packbuilder/pseudonym.py` in the Stoker repo is the normative
//! reference, and `worker/engines/fixtures/format_vectors.json` is the contract.
//! The tests at the bottom read that file, so a change on either side that moves
//! an output fails here. Drift would be silent and nasty: a pack built by one
//! Stoker and replayed by a mismatched worker would stop correlating.
//!
//! Two properties the rotation design leans on, both tested:
//!
//! * **Class stability** — `infer(render(F, x)) == F`. A `hex` output always
//!   begins with a letter from `a-f`, so it can never be read back as digits,
//!   and a `mixed` output always reserves a letter from `g-z`, so it is never
//!   read back as hex. That is what makes two identities in two different
//!   classes impossible to render to the same string.
//! * **Injectivity** — distinct counters below the space size render to distinct
//!   strings, so two passes (or two worker slots) never mint the same identity.
//!
//! Note what is NOT here: mode 1's keyed hash. That happens when the pack is
//! written, so the sample an engine receives already holds the stand-ins and no
//! engine needs HMAC, SHA-2 or big integers. The counter fits comfortably in
//! `u128`, which is why this file has no dependencies at all.

use std::fmt;

const HEX_FIRST: &[u8] = b"abcdef";
/// Radix 20 over `g-z`: guarantees a `mixed` output holds a letter outside
/// `a-f`, so `infer` can never mistake it for hex.
const MIXED_FIRST_LETTER: &[u8] = b"ghijklmnopqrstuvwxyz";
const LOWER: &[u8] = b"abcdefghijklmnopqrstuvwxyz";
const UPPER: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const DEC: &[u8] = b"0123456789";

/// Above this width the modulus `9 * 10^(W-1)` no longer fits in `u128`. A
/// rotation counter is far below `10^37`, so such a width renders by simple
/// decimal placement instead (see `render_digits`).
const DIGITS_U128_MAX: usize = 38;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Case {
    Lower,
    Upper,
}

/// One position of a `mixed` template.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Slot {
    /// Copied through unchanged: a separator, or any non-ASCII-alphanumeric.
    Verbatim(char),
    /// A variable position: its radix and the alphabet to index.
    Choice(u32, &'static [u8]),
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Format {
    Digits(usize),
    Hex(usize),
    Guid,
    Mixed(Vec<Slot>),
    /// Nothing variable at all (`---`): the text is left exactly as it is.
    None,
}

impl fmt::Display for Format {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Format::Digits(w) => write!(f, "digits({w})"),
            Format::Hex(w) => write!(f, "hex({w})"),
            Format::Guid => write!(f, "guid"),
            Format::Mixed(_) => write!(f, "mixed"),
            Format::None => write!(f, "none"),
        }
    }
}

/// `Upper` when the text's letters are all upper case, else `Lower`.
pub fn case_class(text: &str) -> Case {
    let mut any = false;
    for ch in text.chars().filter(|c| c.is_alphabetic()) {
        any = true;
        if !ch.is_uppercase() {
            return Case::Lower;
        }
    }
    if any {
        Case::Upper
    } else {
        Case::Lower
    }
}

/// The value's own format. Rule order is part of the contract: digits, then
/// GUID, then hex, then the general `mixed` template.
pub fn infer(text: &str) -> Format {
    if text.is_empty() {
        return Format::None;
    }
    let bytes = text.as_bytes();
    if bytes.iter().all(|b| b.is_ascii_digit()) {
        return Format::Digits(bytes.len());
    }
    if is_guid(bytes) {
        let letters: Vec<u8> = bytes.iter().copied().filter(|b| b.is_ascii_alphabetic()).collect();
        if letters.is_empty()
            || letters.iter().all(|b| b.is_ascii_lowercase())
            || letters.iter().all(|b| b.is_ascii_uppercase())
        {
            return Format::Guid;
        }
    }
    let hex_lower = bytes.iter().all(|b| b.is_ascii_digit() || (b"abcdef").contains(b))
        && bytes.iter().any(|b| (b"abcdef").contains(b));
    let hex_upper = bytes.iter().all(|b| b.is_ascii_digit() || (b"ABCDEF").contains(b))
        && bytes.iter().any(|b| (b"ABCDEF").contains(b));
    if hex_lower || hex_upper {
        return Format::Hex(bytes.len());
    }
    let mut slots = Vec::with_capacity(text.chars().count());
    let mut first_letter_used = false;
    for ch in text.chars() {
        if ch.is_ascii_digit() {
            slots.push(Slot::Choice(10, DEC));
        } else if ch.is_ascii_lowercase() {
            if first_letter_used {
                slots.push(Slot::Choice(26, LOWER));
            } else {
                slots.push(Slot::Choice(20, MIXED_FIRST_LETTER));
                first_letter_used = true;
            }
        } else if ch.is_ascii_uppercase() {
            if first_letter_used {
                slots.push(Slot::Choice(26, UPPER));
            } else {
                // The upper-case alphabet is derived, not a separate constant,
                // so the two can never drift apart.
                slots.push(Slot::Choice(20, MIXED_FIRST_UPPER));
                first_letter_used = true;
            }
        } else {
            slots.push(Slot::Verbatim(ch));
        }
    }
    if slots.iter().all(|s| matches!(s, Slot::Verbatim(_))) {
        return Format::None;
    }
    Format::Mixed(slots)
}

const MIXED_FIRST_UPPER: &[u8] = b"GHIJKLMNOPQRSTUVWXYZ";

fn is_guid(bytes: &[u8]) -> bool {
    const GROUPS: [usize; 5] = [8, 4, 4, 4, 12];
    let mut parts = bytes.split(|b| *b == b'-');
    for want in GROUPS {
        match parts.next() {
            Some(part) if part.len() == want && part.iter().all(|b| b.is_ascii_hexdigit()) => {}
            _ => return false,
        }
    }
    parts.next().is_none()
}

/// How many distinct strings this format can produce. Saturates rather than
/// overflowing: a space beyond `u128` is "effectively unlimited" for a counter.
pub fn space(format: &Format) -> u128 {
    match format {
        Format::Digits(w) => {
            if *w > DIGITS_U128_MAX {
                u128::MAX
            } else {
                9u128.saturating_mul(pow10(w - 1))
            }
        }
        Format::Hex(w) => 6u128.saturating_mul(pow16(w - 1)),
        Format::Guid => 1u128 << 122,
        Format::Mixed(slots) => slots.iter().fold(1u128, |acc, slot| match slot {
            Slot::Choice(radix, _) => acc.saturating_mul(u128::from(*radix)),
            Slot::Verbatim(_) => acc,
        }),
        Format::None => 1,
    }
}

fn pow10(n: usize) -> u128 {
    (0..n).fold(1u128, |acc, _| acc.saturating_mul(10))
}

fn pow16(n: usize) -> u128 {
    (0..n).fold(1u128, |acc, _| acc.saturating_mul(16))
}

/// Render the counter `x` in `format`. `None` for `Format::None`, which the
/// caller leaves untouched.
pub fn render(format: &Format, x: u128, case: Case) -> Option<String> {
    match format {
        Format::None => None,
        Format::Digits(w) => Some(render_digits(*w, x)),
        Format::Hex(w) => Some(render_hex(*w, x, case)),
        Format::Guid => Some(render_guid(x, case)),
        Format::Mixed(slots) => Some(render_mixed(slots, x)),
    }
}

fn render_digits(width: usize, x: u128) -> String {
    if width <= DIGITS_U128_MAX {
        let m = 9u128 * pow10(width - 1);
        let value = pow10(width - 1) + (x % m);
        return format!("{value}");
    }
    // Beyond u128 the modulus is unreachable for a counter (x << 10^37), so the
    // value is 10^(W-1) + x, whose decimal is "1" then x right-aligned in W-1
    // places. Exact while x < 10^(W-1), which every rotation identity satisfies.
    let mut out = String::with_capacity(width);
    out.push('1');
    let digits = format!("{x}");
    for _ in 0..(width - 1 - digits.len()) {
        out.push('0');
    }
    out.push_str(&digits);
    out
}

fn render_hex(width: usize, x: u128, case: Case) -> String {
    let tail = pow16(width - 1);
    let v = x % (6u128 * tail);
    let (first, rest) = (v / tail, v % tail);
    let mut out = String::with_capacity(width);
    out.push(HEX_FIRST[first as usize] as char);
    if width > 1 {
        out.push_str(&format!("{:0width$x}", rest, width = width - 1));
    }
    if case == Case::Upper {
        out.to_uppercase()
    } else {
        out
    }
}

fn render_guid(x: u128, case: Case) -> String {
    let mut b = x.to_be_bytes();
    b[6] = (b[6] & 0x0F) | 0x40; // version 4
    b[8] = (b[8] & 0x3F) | 0x80; // RFC 4122 variant
    let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
    let out = format!("{}-{}-{}-{}-{}", &hex[0..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..32]);
    if case == Case::Upper {
        out.to_uppercase()
    } else {
        out
    }
}

fn render_mixed(slots: &[Slot], x: u128) -> String {
    let total = slots.iter().fold(1u128, |acc, slot| match slot {
        Slot::Choice(radix, _) => acc.saturating_mul(u128::from(*radix)),
        Slot::Verbatim(_) => acc,
    });
    let mut v = if total == 0 { 0 } else { x % total };
    // Decode least-significant-last, which places the leftmost variable
    // position as the most significant digit (the reference does the same).
    let mut picks = Vec::new();
    for slot in slots.iter().rev() {
        if let Slot::Choice(radix, _) = slot {
            let radix = u128::from(*radix);
            picks.push((v % radix) as usize);
            v /= radix;
        }
    }
    picks.reverse();
    let mut out = String::with_capacity(slots.len());
    let mut i = 0;
    for slot in slots {
        match slot {
            Slot::Verbatim(ch) => out.push(*ch),
            Slot::Choice(_, alphabet) => {
                out.push(alphabet[picks[i]] as char);
                i += 1;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// The shared contract file, written by Stoker's reference implementation.
    fn vectors() -> serde_json::Value {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("fixtures/format_vectors.json");
        if !path.exists() {
            // firebox is also vendored as a submodule under the Stoker repo,
            // where the fixtures live beside the engines.
            path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            path.push("../fixtures/format_vectors.json");
        }
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
        serde_json::from_str(&text).expect("fixture is not valid JSON")
    }

    fn format_of(entry: &serde_json::Value) -> Format {
        if let Some(text) = entry.get("from_text").and_then(|v| v.as_str()) {
            return infer(text);
        }
        let width = entry["width"].as_u64().unwrap() as usize;
        match entry["shape"].as_str().unwrap() {
            "digits" => Format::Digits(width),
            "hex" => Format::Hex(width),
            "guid" => Format::Guid,
            other => panic!("unexpected shape {other}"),
        }
    }

    #[test]
    fn infer_matches_the_reference() {
        let v = vectors();
        for entry in v["infer"].as_array().unwrap() {
            let text = entry["text"].as_str().unwrap();
            let got = infer(text);
            let want_shape = entry["shape"].as_str().unwrap();
            assert_eq!(
                match &got {
                    Format::Digits(_) => "digits",
                    Format::Hex(_) => "hex",
                    Format::Guid => "guid",
                    Format::Mixed(_) => "mixed",
                    Format::None => "none",
                },
                want_shape,
                "infer({text:?})"
            );
            if let Format::Digits(w) | Format::Hex(w) = got {
                assert_eq!(w, entry["width"].as_u64().unwrap() as usize, "width of {text:?}");
            }
            let want_case = match entry["case"].as_str().unwrap() {
                "upper" => Case::Upper,
                _ => Case::Lower,
            };
            assert_eq!(case_class(text), want_case, "case of {text:?}");
            // `space` travels as a string because a GUID's is 2^122.
            let want_space: u128 = entry["space"].as_str().unwrap().parse().unwrap();
            assert_eq!(space(&got), want_space, "space of {text:?}");
        }
    }

    #[test]
    fn rotation_rendering_matches_the_reference() {
        let v = vectors();
        let entries = v["rotate_render"].as_array().unwrap();
        assert!(entries.len() > 300, "expected the full vector set");
        for entry in entries {
            let format = format_of(entry);
            let x: u128 = entry["x"].as_str().unwrap().parse().unwrap();
            let case = match entry["case"].as_str().unwrap() {
                "upper" => Case::Upper,
                _ => Case::Lower,
            };
            let want = entry["out"].as_str().unwrap();
            let got = render(&format, x, case).expect("a renderable format");
            assert_eq!(got, want, "render({format}, {x}, {case:?})");
        }
    }

    #[test]
    fn rendering_is_class_stable() {
        // Rotation proves two identities in two classes cannot collide by
        // relying on this, so it is checked over the whole space, not by example.
        let formats = [
            Format::Digits(1),
            Format::Digits(3),
            Format::Digits(15),
            Format::Digits(48),
            Format::Hex(1),
            Format::Hex(8),
            Format::Hex(16),
            Format::Guid,
            infer("ada.smith"),
            infer("CUST-000123"),
            infer("A1b2"),
        ];
        for format in &formats {
            for x in [0u128, 1, 2, 9, 16, 255, 899, 900, 1 << 20, 1 << 40, u64::MAX as u128] {
                let out = render(format, x, Case::Lower).unwrap();
                let back = infer(&out);
                assert_eq!(
                    std::mem::discriminant(&back),
                    std::mem::discriminant(format),
                    "infer({out:?}) lost the class of {format}"
                );
                if let Format::Digits(w) | Format::Hex(w) = format {
                    assert_eq!(out.chars().count(), *w, "{format} changed width");
                }
                if let Format::Mixed(slots) = format {
                    assert_eq!(infer(&out), Format::Mixed(slots.clone()));
                }
            }
        }
    }

    #[test]
    fn rendering_is_injective_over_small_spaces() {
        // Two passes, or two worker slots, must never mint the same identity.
        for format in [Format::Digits(1), Format::Digits(3), Format::Hex(1), Format::Hex(2), infer("a1"), infer("a-1")]
        {
            let m = space(&format);
            assert!(m <= 200_000, "keep the exhaustive check small");
            let mut seen = std::collections::HashSet::new();
            for x in 0..m {
                assert!(seen.insert(render(&format, x, Case::Lower).unwrap()), "two counters collided in {format}");
            }
            assert_eq!(seen.len() as u128, m);
        }
    }

    #[test]
    fn a_guid_keeps_its_version_and_variant() {
        for x in [0u128, 1, 12345, u64::MAX as u128] {
            let out = render(&Format::Guid, x, Case::Lower).unwrap();
            assert_eq!(out.len(), 36);
            assert_eq!(&out[14..15], "4", "version nibble");
            assert!(matches!(&out[19..20], "8" | "9" | "a" | "b"), "variant nibble");
            assert_eq!(infer(&out), Format::Guid);
        }
    }

    #[test]
    fn digits_never_lead_with_zero_and_keep_their_width() {
        for width in [1usize, 3, 6, 15, 38, 39, 48] {
            for x in [0u128, 1, 899, 1 << 40] {
                let out = render(&Format::Digits(width), x, Case::Lower).unwrap();
                assert_eq!(out.chars().count(), width, "width {width}");
                assert!(!out.starts_with('0'));
                assert!(out.chars().all(|c| c.is_ascii_digit()));
            }
        }
    }

    #[test]
    fn nothing_variable_is_left_alone() {
        for text in ["---", "", "..", "::"] {
            assert_eq!(infer(text), Format::None);
            assert_eq!(render(&Format::None, 7, Case::Lower), None);
        }
    }
}
