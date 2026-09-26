//! Python `re` compatibility over the `regex` crate.
//!
//! eventgen token patterns are Python regexes. Most are plain (`\d{4}-\d{2}`,
//! `##SRCIP##`, `status=(\d{3})`) and compile as-is on the fast `regex` engine.
//! This module papers over the syntax differences (`\Z`, escaped punctuation,
//! a stray `{`, `{,n}`, `(?P=name)`, Python-only flags) and falls back to
//! `fancy-regex` for lookaround and backreferences, which `regex` rejects.

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
    /// Group 1's span when the pattern has a first group and it participated.
    pub group1: Option<(usize, usize)>,
}

impl Span {
    /// The range that a token replacement rewrites: group 1 if it matched,
    /// otherwise the whole match (eventgen's try/except on `match.start(1)`).
    #[inline]
    pub fn target(&self) -> (usize, usize) {
        self.group1.unwrap_or((self.start, self.end))
    }
}

#[derive(Clone)]
pub enum Rx {
    Std { re: regex::Regex, groups: usize },
    Fancy { re: fancy_regex::Regex, groups: usize },
}

impl fmt::Debug for Rx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Rx::Std { re, .. } => write!(f, "Rx::Std({:?})", re.as_str()),
            Rx::Fancy { re, .. } => write!(f, "Rx::Fancy({:?})", re.as_str()),
        }
    }
}

impl Rx {
    /// Compile a Python-syntax pattern.
    pub fn compile(py_pattern: &str) -> Result<Rx, String> {
        let translated = translate(py_pattern);
        match regex::RegexBuilder::new(&translated).octal(true).size_limit(64 * 1024 * 1024).build() {
            Ok(re) => {
                let groups = re.captures_len().saturating_sub(1);
                Ok(Rx::Std { re, groups })
            }
            Err(first) => match fancy_regex::Regex::new(&translated) {
                Ok(re) => {
                    let groups = re.captures_len().saturating_sub(1);
                    log::debug!("pattern {:?} needs the backtracking engine ({})", py_pattern, first);
                    Ok(Rx::Fancy { re, groups })
                }
                Err(e) => Err(format!("invalid regular expression {:?}: {}", py_pattern, e)),
            },
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Rx::Std { re, .. } => re.as_str(),
            Rx::Fancy { re, .. } => re.as_str(),
        }
    }

    pub fn groups(&self) -> usize {
        match self {
            Rx::Std { groups, .. } | Rx::Fancy { groups, .. } => *groups,
        }
    }

    /// All non-overlapping matches, in order (`re.finditer`).
    pub fn find_all(&self, text: &str, out: &mut Vec<Span>) {
        out.clear();
        match self {
            Rx::Std { re, groups } => {
                if *groups == 0 {
                    for m in re.find_iter(text) {
                        out.push(Span { start: m.start(), end: m.end(), group1: None });
                    }
                } else {
                    for c in re.captures_iter(text) {
                        let whole = c.get(0).unwrap();
                        out.push(Span {
                            start: whole.start(),
                            end: whole.end(),
                            group1: c.get(1).map(|g| (g.start(), g.end())),
                        });
                    }
                }
            }
            Rx::Fancy { re, groups } => {
                if *groups == 0 {
                    for m in re.find_iter(text).flatten() {
                        out.push(Span { start: m.start(), end: m.end(), group1: None });
                    }
                } else {
                    for c in re.captures_iter(text).flatten() {
                        let whole = c.get(0).unwrap();
                        out.push(Span {
                            start: whole.start(),
                            end: whole.end(),
                            group1: c.get(1).map(|g| (g.start(), g.end())),
                        });
                    }
                }
            }
        }
    }

    /// First match anywhere (`re.search`).
    pub fn first(&self, text: &str) -> Option<Span> {
        match self {
            Rx::Std { re, groups } => {
                if *groups == 0 {
                    re.find(text).map(|m| Span { start: m.start(), end: m.end(), group1: None })
                } else {
                    re.captures(text).map(|c| {
                        let whole = c.get(0).unwrap();
                        Span { start: whole.start(), end: whole.end(), group1: c.get(1).map(|g| (g.start(), g.end())) }
                    })
                }
            }
            Rx::Fancy { re, groups } => {
                if *groups == 0 {
                    re.find(text).ok().flatten().map(|m| Span { start: m.start(), end: m.end(), group1: None })
                } else {
                    re.captures(text).ok().flatten().map(|c| {
                        let whole = c.get(0).unwrap();
                        Span { start: whole.start(), end: whole.end(), group1: c.get(1).map(|g| (g.start(), g.end())) }
                    })
                }
            }
        }
    }

    pub fn is_match(&self, text: &str) -> bool {
        match self {
            Rx::Std { re, .. } => re.is_match(text),
            Rx::Fancy { re, .. } => re.is_match(text).unwrap_or(false),
        }
    }
}

/// A per-thread handle on a regex. `regex::Regex` shares one cache pool
/// across threads with a lock-free fast path only for the owning thread, so
/// every worker clones the regex (cheap) and keeps its own `CaptureLocations`
/// to find group spans without allocating per match.
pub struct RxLocal {
    rx: Rx,
    locs: Option<regex::CaptureLocations>,
}

impl RxLocal {
    pub fn new(rx: &Rx) -> RxLocal {
        let rx = rx.clone();
        let locs = match &rx {
            Rx::Std { re, groups } if *groups > 0 => Some(re.capture_locations()),
            _ => None,
        };
        RxLocal { rx, locs }
    }

    pub fn rx(&self) -> &Rx {
        &self.rx
    }

    /// All non-overlapping matches, in order (`re.finditer`).
    pub fn find_all(&mut self, text: &str, out: &mut Vec<Span>) {
        match (&self.rx, &mut self.locs) {
            (Rx::Std { re, groups }, Some(locs)) if *groups > 0 => {
                out.clear();
                let mut start = 0;
                while start <= text.len() {
                    let Some(m) = re.captures_read_at(locs, text, start) else { break };
                    out.push(Span { start: m.start(), end: m.end(), group1: locs.get(1) });
                    start = if m.end() == m.start() { next_boundary(text, m.end()) } else { m.end() };
                }
            }
            _ => self.rx.find_all(text, out),
        }
    }
}

fn next_boundary(text: &str, i: usize) -> usize {
    let mut j = i + 1;
    while j < text.len() && !text.is_char_boundary(j) {
        j += 1;
    }
    j
}

/// `re.match(pattern, name)` whose match spans the whole name: how eventgen
/// matches a stanza name against the files in the sample directory.
pub fn full_match(pattern: &str, name: &str) -> bool {
    let anchored = format!("^(?:{})", translate(pattern));
    let hit = match regex::RegexBuilder::new(&anchored).octal(true).build() {
        Ok(re) => re.find(name).map(|m| (m.start(), m.end())),
        Err(_) => match fancy_regex::Regex::new(&anchored) {
            Ok(re) => re.find(name).ok().flatten().map(|m| (m.start(), m.end())),
            Err(_) => None,
        },
    };
    matches!(hit, Some((0, end)) if end == name.len())
}

/// Characters the `regex` crate treats as meta (and therefore accepts escaped).
fn is_meta(c: char) -> bool {
    matches!(
        c,
        '\\' | '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$' | '#' | '&' | '-' | '~'
    )
}

/// Rewrite Python regex syntax into `regex`/`fancy-regex` syntax.
pub fn translate(py: &str) -> String {
    let chars: Vec<char> = py.chars().collect();
    let mut out = String::with_capacity(py.len() + 8);
    let mut i = 0;
    let mut in_class = false;
    let mut class_start = 0usize;
    while i < chars.len() {
        let c = chars[i];
        if c == '\\' {
            if i + 1 >= chars.len() {
                out.push_str("\\\\");
                i += 1;
                continue;
            }
            let n = chars[i + 1];
            match n {
                'Z' if !in_class => out.push_str("\\z"),
                'Z' => out.push_str("\\z"),
                c2 if c2.is_ascii_alphanumeric() => {
                    out.push('\\');
                    out.push(c2);
                }
                c2 if is_meta(c2) => {
                    out.push('\\');
                    out.push(c2);
                }
                c2 if c2.is_ascii_punctuation() || c2 == ' ' => {
                    // Python: an escaped non-meta punctuation is that literal
                    out.push(c2);
                }
                c2 => {
                    out.push('\\');
                    out.push(c2);
                }
            }
            i += 2;
            continue;
        }
        if in_class {
            if c == ']' && i > class_start {
                in_class = false;
            }
            if c == '[' && i + 1 < chars.len() && chars[i + 1] == ':' {
                // Python has no POSIX classes: `[[:alpha:]]` is literal there.
                out.push_str("\\[");
                i += 1;
                continue;
            }
            out.push(c);
            i += 1;
            continue;
        }
        match c {
            '[' => {
                in_class = true;
                out.push(c);
                i += 1;
                // a leading `^` and/or `]` are literal parts of the class
                if i < chars.len() && chars[i] == '^' {
                    out.push('^');
                    i += 1;
                }
                class_start = i;
                if i < chars.len() && chars[i] == ']' {
                    out.push_str("\\]");
                    i += 1;
                    class_start = i;
                }
            }
            '{' => {
                // valid quantifier forms: {m} {m,} {m,n} {,n}
                let mut j = i + 1;
                let mut lo = String::new();
                while j < chars.len() && chars[j].is_ascii_digit() {
                    lo.push(chars[j]);
                    j += 1;
                }
                let mut hi: Option<String> = None;
                if j < chars.len() && chars[j] == ',' {
                    j += 1;
                    let mut h = String::new();
                    while j < chars.len() && chars[j].is_ascii_digit() {
                        h.push(chars[j]);
                        j += 1;
                    }
                    hi = Some(h);
                }
                let valid_close = j < chars.len() && chars[j] == '}';
                let valid = valid_close && (!lo.is_empty() || hi.as_ref().map(|h| !h.is_empty()).unwrap_or(false));
                if valid {
                    out.push('{');
                    if lo.is_empty() {
                        out.push('0');
                    } else {
                        out.push_str(&lo);
                    }
                    if let Some(h) = hi {
                        out.push(',');
                        out.push_str(&h);
                    }
                    out.push('}');
                    i = j + 1;
                } else {
                    out.push_str("\\{");
                    i += 1;
                }
            }
            '}' => {
                out.push_str("\\}");
                i += 1;
            }
            '(' if i + 2 < chars.len() && chars[i + 1] == '?' => {
                // (?P=name) -> \k<name> ; strip Python-only flags u/a/L
                if chars[i + 2] == 'P' && i + 3 < chars.len() && chars[i + 3] == '=' {
                    if let Some(close) = chars[i + 4..].iter().position(|&x| x == ')') {
                        let name: String = chars[i + 4..i + 4 + close].iter().collect();
                        out.push_str("\\k<");
                        out.push_str(&name);
                        out.push('>');
                        i += 4 + close + 1;
                        continue;
                    }
                }
                let mut j = i + 2;
                let mut flags = String::new();
                while j < chars.len() && chars[j].is_ascii_alphabetic() {
                    flags.push(chars[j]);
                    j += 1;
                }
                if j < chars.len() && chars[j] == ')' && !flags.is_empty() {
                    let kept: String = flags.chars().filter(|f| !matches!(f, 'u' | 'a' | 'L')).collect();
                    if !kept.is_empty() {
                        out.push_str("(?");
                        out.push_str(&kept);
                        out.push(')');
                    }
                    i = j + 1;
                    continue;
                }
                out.push(c);
                i += 1;
            }
            _ => {
                out.push(c);
                i += 1;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn translation_rules() {
        assert_eq!(translate(r"\d{4}-\d{2}"), r"\d{4}-\d{2}");
        assert_eq!(translate(r"a{,3}"), r"a{0,3}");
        assert_eq!(translate(r#"{"a": (\d+)}"#), r#"\{"a": (\d+)\}"#);
        assert_eq!(translate(r"\Z"), r"\z");
        assert_eq!(translate(r"a\/b\'c\-d"), r"a/b'c\-d");
        assert_eq!(translate(r"(?P=x)"), r"\k<x>");
        assert_eq!(translate(r"(?iu)x"), r"(?i)x");
        assert_eq!(translate(r"(?u)x"), r"x");
        assert_eq!(translate(r"[]a]"), r"[\]a]");
        assert_eq!(translate(r"[^]a]"), r"[^\]a]");
        assert_eq!(translate(r"[\]a]"), r"[\]a]");
    }

    #[test]
    fn compile_and_match_group_semantics() {
        let rx = Rx::compile(r"status=(\d{3})").unwrap();
        let mut spans = Vec::new();
        rx.find_all("a status=200 b status=404", &mut spans);
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0].target(), (9, 12));
        assert_eq!(spans[1].target(), (22, 25));
        let rx = Rx::compile(r"##SRCIP##").unwrap();
        rx.find_all("x ##SRCIP## y ##SRCIP##", &mut spans);
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0].target(), (2, 11));
    }

    #[test]
    fn optional_group_falls_back_to_whole_match() {
        let rx = Rx::compile(r"a(b)?c").unwrap();
        let mut spans = Vec::new();
        rx.find_all("ac abc", &mut spans);
        assert_eq!(spans[0].target(), (0, 2));
        assert_eq!(spans[1].target(), (4, 5));
    }

    #[test]
    fn lookaround_uses_fancy_engine() {
        let rx = Rx::compile(r"(?<=id=)\d+").unwrap();
        assert!(matches!(rx, Rx::Fancy { .. }));
        let mut spans = Vec::new();
        rx.find_all("id=42 x=7", &mut spans);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].target(), (3, 5));
    }

    #[test]
    fn stanza_full_match() {
        assert!(full_match("web_access.sample", "web_access.sample"));
        assert!(full_match(r"web.*", "web_access.sample"));
        assert!(!full_match("web", "web_access.sample"));
        assert!(!full_match("access", "web_access.sample"));
        assert!(full_match(".*", "anything"));
    }

    #[test]
    fn invalid_pattern_errors() {
        assert!(Rx::compile(r"(").is_err());
    }
}
