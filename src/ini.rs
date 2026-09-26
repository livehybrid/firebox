//! A `configparser.RawConfigParser`-compatible INI reader.
//!
//! eventgen reads its conf files with Python's `RawConfigParser` (delimiters
//! `=` and `:`, case-preserving keys via `optionxform = str`, `#`/`;` full-line
//! comments, indented continuation lines, and a `DEFAULT` section whose keys
//! are merged into every other section). This module reproduces exactly that,
//! leniently: duplicate sections merge and a duplicate key keeps its last value
//! (the `strict = False` behaviour Stoker's conf rewriter relies on).

use std::collections::HashMap;
use std::fmt;
use std::path::Path;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Section {
    pub name: String,
    /// Key/value pairs in file order. Lookups go through [`Section::get`].
    pub items: Vec<(String, String)>,
}

impl Section {
    pub fn get(&self, key: &str) -> Option<&str> {
        // Last value wins, mirroring configparser's overwrite-on-duplicate.
        self.items.iter().rev().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
    }

    pub fn set(&mut self, key: &str, value: &str) {
        if let Some(slot) = self.items.iter_mut().rev().find(|(k, _)| k == key) {
            slot.1 = value.to_string();
        } else {
            self.items.push((key.to_string(), value.to_string()));
        }
    }

    pub fn remove(&mut self, key: &str) {
        self.items.retain(|(k, _)| k != key);
    }

    /// Items as a map, later duplicates overriding earlier ones.
    pub fn to_map(&self) -> HashMap<String, String> {
        let mut map = HashMap::with_capacity(self.items.len());
        for (k, v) in &self.items {
            map.insert(k.clone(), v.clone());
        }
        map
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Ini {
    /// Sections in file order, excluding `DEFAULT`.
    pub sections: Vec<Section>,
    /// The `DEFAULT` section (configparser's default section), merged into every
    /// section by [`Ini::section_map`].
    pub defaults: Section,
}

#[derive(Debug)]
pub struct IniError {
    pub line: usize,
    pub message: String,
}

impl fmt::Display for IniError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "line {}: {}", self.line, self.message)
    }
}

impl std::error::Error for IniError {}

impl Ini {
    pub fn read_file(path: &Path) -> anyhow::Result<Ini> {
        let bytes =
            std::fs::read(path).map_err(|e| anyhow::anyhow!("cannot read conf file {}: {}", path.display(), e))?;
        let text = String::from_utf8_lossy(&bytes);
        parse_str(&text).map_err(|e| anyhow::anyhow!("{}: {}", path.display(), e))
    }

    pub fn section(&self, name: &str) -> Option<&Section> {
        self.sections.iter().find(|s| s.name == name)
    }

    pub fn section_mut(&mut self, name: &str) -> Option<&mut Section> {
        self.sections.iter_mut().find(|s| s.name == name)
    }

    /// The effective key/value map of one section: `DEFAULT` keys first, then
    /// the section's own keys (configparser `items(section)` semantics).
    pub fn section_map(&self, section: &Section) -> HashMap<String, String> {
        let mut map = self.defaults.to_map();
        for (k, v) in &section.items {
            map.insert(k.clone(), v.clone());
        }
        map
    }

    /// Serialise in the same shape `RawConfigParser.write` produces
    /// (`key = value`, blank line between sections, multi-line values
    /// indented with a tab).
    pub fn to_string(&self) -> String {
        let mut out = String::new();
        let mut write_section = |name: &str, items: &[(String, String)]| {
            out.push('[');
            out.push_str(name);
            out.push_str("]\n");
            for (k, v) in items {
                out.push_str(k);
                out.push_str(" = ");
                out.push_str(&v.replace('\n', "\n\t"));
                out.push('\n');
            }
            out.push('\n');
        };
        if !self.defaults.items.is_empty() {
            write_section("DEFAULT", &self.defaults.items);
        }
        for s in &self.sections {
            write_section(&s.name, &s.items);
        }
        out
    }
}

/// Parse INI text with `RawConfigParser` semantics.
pub fn parse_str(text: &str) -> Result<Ini, IniError> {
    let mut ini = Ini::default();
    // index into ini.sections, or None for DEFAULT / no section yet
    enum Cur {
        None,
        Default,
        Section(usize),
    }
    let mut cur = Cur::None;
    let mut cur_key: Option<String> = None;
    let mut indent_level: usize = usize::MAX;

    for (idx, raw_line) in text.lines().enumerate() {
        let lineno = idx + 1;
        let line = raw_line.trim_end_matches('\r');
        let stripped = line.trim();
        // Full-line comments and blank lines end any continuation.
        if stripped.is_empty() || stripped.starts_with('#') || stripped.starts_with(';') {
            indent_level = usize::MAX;
            continue;
        }
        let cur_indent = line.len() - line.trim_start().len();
        // Continuation line: more indented than the key it belongs to.
        if cur_indent > indent_level {
            if let Some(key) = &cur_key {
                let target = match cur {
                    Cur::Default => Some(&mut ini.defaults),
                    Cur::Section(i) => Some(&mut ini.sections[i]),
                    Cur::None => None,
                };
                if let Some(section) = target {
                    if let Some(slot) = section.items.iter_mut().rev().find(|(k, _)| k == key) {
                        slot.1.push('\n');
                        slot.1.push_str(stripped);
                        continue;
                    }
                }
            }
        }
        indent_level = cur_indent;
        if let Some(rest) = stripped.strip_prefix('[') {
            // configparser: `\[(?P<header>.+)\]` matched at the line start, so
            // the header is everything up to the LAST `]`.
            if let Some(end) = rest.rfind(']') {
                let name = &rest[..end];
                if name == "DEFAULT" {
                    cur = Cur::Default;
                } else {
                    let pos = match ini.sections.iter().position(|s| s.name == name) {
                        Some(p) => p,
                        None => {
                            ini.sections.push(Section { name: name.to_string(), items: Vec::new() });
                            ini.sections.len() - 1
                        }
                    };
                    cur = Cur::Section(pos);
                }
                cur_key = None;
                continue;
            }
            return Err(IniError { line: lineno, message: format!("malformed section header: {}", stripped) });
        }
        // Option line: split on the first `=` or `:`.
        let delim = stripped.find(['=', ':']);
        let (key, value) = match delim {
            Some(pos) => (stripped[..pos].trim(), stripped[pos + 1..].trim()),
            None => {
                return Err(IniError { line: lineno, message: format!("expected `key = value`, got: {}", stripped) })
            }
        };
        if key.is_empty() {
            return Err(IniError { line: lineno, message: format!("empty key in: {}", stripped) });
        }
        let section = match cur {
            Cur::Default => &mut ini.defaults,
            Cur::Section(i) => &mut ini.sections[i],
            Cur::None => {
                return Err(IniError {
                    line: lineno,
                    message: format!("key `{}` appears before any section header", key),
                })
            }
        };
        section.items.push((key.to_string(), value.to_string()));
        cur_key = Some(key.to_string());
    }
    Ok(ini)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sections_keys_and_comments() {
        let text = "# c\n[global]\nkey = v\n; c2\n[s.sample]\ntoken.0.token = a=b:c\nx: y\n";
        let ini = parse_str(text).unwrap();
        assert_eq!(ini.sections.len(), 2);
        assert_eq!(ini.section("global").unwrap().get("key"), Some("v"));
        // first delimiter wins: `=`
        assert_eq!(ini.section("s.sample").unwrap().get("token.0.token"), Some("a=b:c"));
        assert_eq!(ini.section("s.sample").unwrap().get("x"), Some("y"));
    }

    #[test]
    fn continuation_lines_join_with_newline() {
        let text = "[s]\nhourOfDayRate = {\"0\": 1,\n    \"1\": 2}\nnext = 1\n";
        let ini = parse_str(text).unwrap();
        assert_eq!(ini.section("s").unwrap().get("hourOfDayRate"), Some("{\"0\": 1,\n\"1\": 2}"));
        assert_eq!(ini.section("s").unwrap().get("next"), Some("1"));
    }

    #[test]
    fn default_section_merges_into_sections() {
        let text = "[DEFAULT]\nindex = main\n[s]\ncount = 1\n";
        let ini = parse_str(text).unwrap();
        let map = ini.section_map(ini.section("s").unwrap());
        assert_eq!(map.get("index").map(String::as_str), Some("main"));
        assert_eq!(map.get("count").map(String::as_str), Some("1"));
    }

    #[test]
    fn duplicate_key_last_wins_and_sections_merge() {
        let text = "[s]\na = 1\na = 2\n[t]\nb = 1\n[s]\nc = 3\n";
        let ini = parse_str(text).unwrap();
        assert_eq!(ini.sections.len(), 2);
        assert_eq!(ini.section("s").unwrap().get("a"), Some("2"));
        assert_eq!(ini.section("s").unwrap().get("c"), Some("3"));
    }

    #[test]
    fn keys_are_case_sensitive() {
        let text = "[s]\nCount = 1\ncount = 2\n";
        let ini = parse_str(text).unwrap();
        assert_eq!(ini.section("s").unwrap().get("Count"), Some("1"));
        assert_eq!(ini.section("s").unwrap().get("count"), Some("2"));
    }

    #[test]
    fn roundtrip_write() {
        let text = "[s]\na = 1\nb = x\n";
        let ini = parse_str(text).unwrap();
        assert_eq!(ini.to_string(), "[s]\na = 1\nb = x\n\n");
    }
}
