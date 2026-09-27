//! Sample file loading (`lib/eventgensamples.py Sample.loadSample`).
//!
//! Raw samples are one event per line with the default breaker, or split by a
//! breaker regex (each event keeps its breaker). CSV samples carry `_raw` plus
//! optional per-row `index`/`host`/`source`/`sourcetype`/`hostRegex` columns.
//! Files that are not UTF-8 are decoded as latin-1, like upstream's fallback.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use crate::envelope::Meta;
use crate::pyre::Rx;

pub const DEFAULT_BREAKER: &str = r"[^\r\n\s]+";

#[derive(Debug, Clone)]
pub struct Line {
    /// Event text without its trailing newline.
    pub raw: String,
    /// `len(_raw)` as eventgen counts it (with the newline), in bytes.
    pub size: usize,
    /// Per-row metadata from a CSV sample; `None` means "use the sample's".
    pub meta: Option<Arc<Meta>>,
    /// A CSV `_time` column, if any (used by replay's `timeField`).
    pub time_field: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct SampleData {
    pub lines: Vec<Line>,
    /// Prefix sums of `size` for the perdayvolume fill.
    pub prefix: Vec<u64>,
}

impl SampleData {
    pub fn len(&self) -> usize {
        self.lines.len()
    }

    pub fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }

    pub fn total_size(&self) -> u64 {
        self.prefix.last().copied().unwrap_or(0)
    }

    fn finish(mut self) -> SampleData {
        let mut acc = 0u64;
        self.prefix = Vec::with_capacity(self.lines.len() + 1);
        self.prefix.push(0);
        for l in &self.lines {
            acc += l.size as u64;
            self.prefix.push(acc);
        }
        self
    }
}

pub fn read_text(path: &Path) -> anyhow::Result<String> {
    let bytes = std::fs::read(path).map_err(|e| anyhow::anyhow!("cannot read sample {}: {}", path.display(), e))?;
    Ok(decode(bytes))
}

/// UTF-8, else latin-1 (every byte is a code point), matching upstream.
pub fn decode(bytes: Vec<u8>) -> String {
    match String::from_utf8(bytes) {
        Ok(s) => s,
        Err(e) => e.into_bytes().iter().map(|&b| b as char).collect(),
    }
}

/// Python text-mode universal newlines: `\r\n` and lone `\r` become `\n`.
fn normalise_newlines(text: &str) -> String {
    if !text.contains('\r') {
        return text.to_string();
    }
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\r' {
            if chars.peek() == Some(&'\n') {
                chars.next();
            }
            out.push('\n');
        } else {
            out.push(c);
        }
    }
    out
}

pub fn load_raw(path: &Path, breaker: &str) -> anyhow::Result<SampleData> {
    let text = normalise_newlines(&read_text(path)?);
    let pieces: Vec<String> = if breaker == DEFAULT_BREAKER {
        split_lines_keep_newline(&text)
    } else {
        // eventgen compiles the breaker with re.M: `^` and `$` are per line.
        match Rx::compile(&format!("(?m){}", breaker)) {
            Ok(rx) => split_by_breaker(&text, &rx),
            Err(e) => {
                log::error!(
                    "Line breaker '{}' for sample {} could not be compiled ({}); using default breaker",
                    breaker,
                    path.display(),
                    e
                );
                split_lines_keep_newline(&text)
            }
        }
    };
    let mut data = SampleData::default();
    for mut piece in pieces {
        if piece == "\n" {
            continue;
        }
        if !piece.ends_with('\n') {
            piece.push('\n');
        }
        let size = piece.len();
        let raw = piece.trim_end_matches(['\r', '\n']).to_string();
        data.lines.push(Line { raw, size, meta: None, time_field: None });
    }
    Ok(data.finish())
}

/// `readlines()`: every line with its trailing newline; the final unterminated
/// line (if any) without one; an empty file yields nothing.
fn split_lines_keep_newline(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut start = 0;
    let bytes = text.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        if b == b'\n' {
            out.push(text[start..=i].to_string());
            start = i + 1;
        }
    }
    if start < text.len() {
        out.push(text[start..].to_string());
    }
    out
}

/// eventgen's breaker loop: events run from one breaker match to the next
/// (a match at offset 0 does not open an empty leading event).
fn split_by_breaker(text: &str, rx: &Rx) -> Vec<String> {
    let mut spans = Vec::new();
    rx.find_all(text, &mut spans);
    let mut out = Vec::new();
    let mut extract = 0usize;
    for sp in &spans {
        if sp.start != 0 {
            out.push(text[extract..sp.start].to_string());
            extract = sp.start;
        }
    }
    out.push(text[extract..].to_string());
    out
}

pub fn load_csv(path: &Path, defaults: &Meta, default_host_regex: Option<&str>) -> anyhow::Result<SampleData> {
    let text = read_text(path)?;
    let rows = parse_csv(&text);
    let Some(header) = rows.first() else {
        return Ok(SampleData::default().finish());
    };
    let col = |name: &str| header.iter().position(|h| h == name);
    let Some(raw_col) = col("_raw") else {
        anyhow::bail!("csv sample {} has no _raw column", path.display());
    };
    let index_col = col("index");
    let host_col = col("host");
    let source_col = col("source");
    let sourcetype_col = col("sourcetype");
    let time_col = col("_time");
    let _ = default_host_regex;
    let mut data = SampleData::default();
    for row in rows.iter().skip(1) {
        if row.len() <= raw_col {
            log::error!("Missing _raw in csv row {:?}", row);
            continue;
        }
        let mut raw = row[raw_col].clone();
        if !raw.ends_with('\n') {
            raw.push('\n');
        }
        let size = raw.len();
        let pick = |c: Option<usize>, d: &Option<Arc<str>>| -> Option<Arc<str>> {
            match c {
                Some(i) if i < row.len() => Some(Arc::from(row[i].as_str())),
                _ => d.clone(),
            }
        };
        let meta = Meta {
            index: pick(index_col, &defaults.index),
            host: pick(host_col, &defaults.host),
            source: pick(source_col, &defaults.source),
            sourcetype: pick(sourcetype_col, &defaults.sourcetype),
        };
        data.lines.push(Line {
            raw: raw.trim_end_matches(['\r', '\n']).to_string(),
            size,
            meta: Some(Arc::new(meta)),
            time_field: time_col.and_then(|i| row.get(i).cloned()),
        });
    }
    Ok(data.finish())
}

/// A small RFC 4180 reader (quoted fields, doubled quotes, embedded newlines).
fn parse_csv(text: &str) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    let mut row: Vec<String> = Vec::new();
    let mut field = String::new();
    let mut in_quotes = false;
    let mut chars = text.chars().peekable();
    let mut any = false;
    while let Some(c) = chars.next() {
        any = true;
        if in_quotes {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    field.push('"');
                } else {
                    in_quotes = false;
                }
            } else {
                field.push(c);
            }
            continue;
        }
        match c {
            '"' => in_quotes = true,
            ',' => row.push(std::mem::take(&mut field)),
            '\r' => {
                if chars.peek() == Some(&'\n') {
                    chars.next();
                }
                row.push(std::mem::take(&mut field));
                rows.push(std::mem::take(&mut row));
            }
            '\n' => {
                row.push(std::mem::take(&mut field));
                rows.push(std::mem::take(&mut row));
            }
            _ => field.push(c),
        }
    }
    if any && (!field.is_empty() || !row.is_empty()) {
        row.push(field);
        rows.push(row);
    }
    rows
}

/// `extendIndexes = "idx1, idx2, prefix:3"` -> `[idx1, idx2, prefix0, prefix1, prefix2]`.
pub fn parse_extend_indexes(spec: &str) -> Vec<String> {
    let mut out = Vec::new();
    for item in spec.split(',') {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        if let Some((prefix, n)) = item.rsplit_once(':') {
            match n.trim().parse::<usize>() {
                Ok(count) => {
                    for i in 0..count {
                        out.push(format!("{}{}", prefix, i));
                    }
                }
                Err(_) => {
                    log::error!("Failed to parse extendIndexes item {:?}", item);
                    return Vec::new();
                }
            }
        } else {
            out.push(item.to_string());
        }
    }
    out
}

#[allow(dead_code)]
pub fn line_map(lines: &[Line]) -> HashMap<usize, &Line> {
    lines.iter().enumerate().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn tmpfile(name: &str, body: &[u8]) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("firebox-sample-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join(name);
        std::fs::File::create(&p).unwrap().write_all(body).unwrap();
        p
    }

    #[test]
    fn raw_lines_skip_blank_and_keep_last() {
        let p = tmpfile("a.sample", b"one\n\ntwo\r\nthree");
        let d = load_raw(&p, DEFAULT_BREAKER).unwrap();
        let raws: Vec<&str> = d.lines.iter().map(|l| l.raw.as_str()).collect();
        assert_eq!(raws, vec!["one", "two", "three"]);
        assert_eq!(d.lines[0].size, 4);
        assert_eq!(d.lines[2].size, 6);
        assert_eq!(d.total_size(), 14);
    }

    #[test]
    fn breaker_splits_multiline_events() {
        let p = tmpfile("b.sample", b"2026-01-01 a\n  cont\n2026-01-02 b\n");
        let d = load_raw(&p, r"\d{4}-\d{2}-\d{2}").unwrap();
        let raws: Vec<&str> = d.lines.iter().map(|l| l.raw.as_str()).collect();
        assert_eq!(raws, vec!["2026-01-01 a\n  cont", "2026-01-02 b"]);
    }

    #[test]
    fn anchored_breaker_is_multiline() {
        let p = tmpfile("m.sample", b"2026-01-01 a\n  at x\n  at y\n2026-01-02 b\n<Event>\n</Event>\n");
        let d = load_raw(&p, r"^(?:\d{4}-\d{2}-\d{2}|<Event)").unwrap();
        let raws: Vec<&str> = d.lines.iter().map(|l| l.raw.as_str()).collect();
        assert_eq!(raws, vec!["2026-01-01 a\n  at x\n  at y", "2026-01-02 b", "<Event>\n</Event>"]);
    }

    #[test]
    fn latin1_fallback() {
        let p = tmpfile("c.sample", b"caf\xe9\n");
        let d = load_raw(&p, DEFAULT_BREAKER).unwrap();
        assert_eq!(d.lines[0].raw, "café");
    }

    #[test]
    fn csv_rows_with_meta() {
        let p = tmpfile("d.csv", b"_raw,host,sourcetype\n\"a,b\",h1,st1\nplain,,st2\n");
        let defaults = Meta { index: Some("main".into()), host: Some("dh".into()), source: None, sourcetype: None };
        let d = load_csv(&p, &defaults, None).unwrap();
        assert_eq!(d.lines.len(), 2);
        assert_eq!(d.lines[0].raw, "a,b");
        let m0 = d.lines[0].meta.as_ref().unwrap();
        assert_eq!(m0.host.as_deref(), Some("h1"));
        assert_eq!(m0.sourcetype.as_deref(), Some("st1"));
        assert_eq!(m0.index.as_deref(), Some("main"));
        let m1 = d.lines[1].meta.as_ref().unwrap();
        assert_eq!(m1.host.as_deref(), Some(""));
    }

    #[test]
    fn extend_indexes() {
        assert_eq!(parse_extend_indexes("a, b,p:2"), vec!["a", "b", "p0", "p1"]);
        assert!(parse_extend_indexes("p:x").is_empty());
    }
}
