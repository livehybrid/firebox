//! eventgen.conf loading: layering, stanza-to-file matching and the
//! post-processing rules of `lib/eventgenconfig.py Config.parse`.
//!
//! The result is one [`StanzaConf`] per (stanza, matched sample file) with a
//! fully layered settings map (packaged global defaults <- `[global]` <-
//! `DEFAULT` <- inherited stanzas <- the stanza) plus its ordered tokens.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::ini::Ini;
use crate::pyre;

/// The `[global]` stanza of eventgen's packaged `default/eventgen.conf`.
pub const GLOBAL_DEFAULTS: &[(&str, &str)] = &[
    ("disabled", "false"),
    ("debug", "false"),
    ("verbosity", "false"),
    ("spoolDir", "$SPLUNK_HOME/var/spool/splunk"),
    ("spoolFile", "<SAMPLE>"),
    ("breaker", r"[^\r\n\s]+"),
    ("mode", "sample"),
    ("sampletype", "raw"),
    ("interval", "60"),
    ("delay", "0"),
    ("timeMultiple", "1"),
    ("count", "-1"),
    ("earliest", "now"),
    ("latest", "now"),
    ("randomizeEvents", "false"),
    ("outputMode", "modinput"),
    ("fileMaxBytes", "10485760"),
    ("fileBackupFiles", "5"),
    ("splunkPort", "8089"),
    ("splunkMethod", "https"),
    ("index", "main"),
    ("sourcetype", "eventgen"),
    ("host", "127.0.0.1"),
    ("generator", "default"),
    ("rater", "config"),
    ("generatorWorkers", "1"),
    ("outputWorkers", "1"),
    ("timeField", "_raw"),
    ("threading", "thread"),
    ("profiler", "false"),
    ("maxIntervalsBeforeFlush", "3"),
    ("maxQueueLength", "0"),
    ("useOutputQueue", "false"),
    ("autotimestamp", "false"),
    ("httpeventWaitResponse", "true"),
    ("disableLoggingQueue", "true"),
    ("splitSample", "0"),
];

pub const VALID_REPLACEMENT_TYPES: &[&str] =
    &["static", "timestamp", "replaytimestamp", "random", "rated", "file", "mvfile", "seqfile", "integerid"];

/// Settings that are never inherited from another stanza during flattening.
const NON_FLATTEN_KEYS: &[&str] = &["eai:acl", "blacklist", "disabled", "name"];

#[derive(Debug, Clone, PartialEq)]
pub struct TokenSpec {
    pub index: usize,
    pub token: String,
    pub replacement_type: String,
    pub replacement: String,
}

#[derive(Debug, Clone)]
pub struct StanzaConf {
    /// The stanza name as written (a regex over sample file names).
    pub stanza: String,
    /// eventgen's `sample.name` after matching: the file name, else the stanza.
    pub name: String,
    pub file_path: Option<PathBuf>,
    pub sample_dir: PathBuf,
    pub settings: HashMap<String, String>,
    pub tokens: Vec<TokenSpec>,
    /// `host.token` / `host.replacement` (always a `file` replacement).
    pub host_token: Option<(String, String)>,
    /// Keys the stanza (or an inherited stanza) set explicitly, as opposed to
    /// values that came from the global layer.
    explicit: HashSet<String>,
}

impl StanzaConf {
    pub fn get(&self, key: &str) -> Option<&str> {
        self.settings.get(key).map(String::as_str)
    }

    pub fn get_str(&self, key: &str, default: &str) -> String {
        self.get(key).unwrap_or(default).to_string()
    }

    pub fn get_i64(&self, key: &str) -> Option<i64> {
        self.get(key)
            .and_then(|v| v.trim().parse::<i64>().ok().or_else(|| v.trim().parse::<f64>().ok().map(|f| f as i64)))
    }

    pub fn get_f64(&self, key: &str) -> Option<f64> {
        self.get(key).and_then(|v| v.trim().parse::<f64>().ok())
    }

    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.get(key).map(parse_bool)
    }

    pub fn set(&mut self, key: &str, value: &str) {
        self.settings.insert(key.to_string(), value.to_string());
        self.explicit.insert(key.to_string());
    }

    pub fn remove(&mut self, key: &str) {
        self.settings.remove(key);
    }

    pub fn is_explicit(&self, key: &str) -> bool {
        self.explicit.contains(key)
    }

    pub fn mode(&self) -> &str {
        self.get("mode").unwrap_or("sample")
    }

    pub fn generator(&self) -> &str {
        self.get("generator").unwrap_or("default")
    }

    pub fn output_mode(&self) -> &str {
        self.get("outputMode").unwrap_or("stdout")
    }
}

/// eventgen's bool parsing: `'0'`, `'false'`, `'False'` are false, anything
/// else non-empty is true.
pub fn parse_bool(v: &str) -> bool {
    let t = v.trim();
    !(t.is_empty() || t == "0" || t == "false" || t == "False")
}

#[derive(Debug, Clone, Default)]
pub struct LoadOptions {
    /// `-s`: run only this stanza.
    pub sample: Option<String>,
    pub override_count: Option<i64>,
    pub override_interval: Option<i64>,
    pub override_end: Option<String>,
    pub override_backfill: Option<String>,
    /// Force `outputMode` (`--devnull`, `--stdout`).
    pub override_output: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Conf {
    pub conf_path: PathBuf,
    pub global: HashMap<String, String>,
    pub samples: Vec<StanzaConf>,
}

impl Conf {
    pub fn global_i64(&self, key: &str) -> Option<i64> {
        self.global.get(key).and_then(|v| v.trim().parse().ok())
    }
}

/// Load and resolve an eventgen.conf (a file, or an app directory holding
/// `default/eventgen.conf` and optionally `local/eventgen.conf`).
pub fn load(conf_path: &Path, opts: &LoadOptions) -> anyhow::Result<Conf> {
    let conf_path =
        conf_path.canonicalize().map_err(|e| anyhow::anyhow!("cannot read conf {}: {}", conf_path.display(), e))?;
    let (ini, conf_dir, default_sample_dir) = if conf_path.is_dir() {
        let mut merged = Ini::default();
        for rel in ["default/eventgen.conf", "local/eventgen.conf"] {
            let p = conf_path.join(rel);
            if p.is_file() {
                merge_ini(&mut merged, Ini::read_file(&p)?);
            }
        }
        if merged.sections.is_empty() {
            anyhow::bail!("no eventgen.conf under {}", conf_path.display());
        }
        (merged, conf_path.join("default"), conf_path.join("samples"))
    } else {
        let dir = conf_path.parent().map(Path::to_path_buf).unwrap_or_else(|| PathBuf::from("."));
        let base = dir.parent().map(Path::to_path_buf).unwrap_or_else(|| dir.clone());
        (Ini::read_file(&conf_path)?, dir, base.join("samples"))
    };

    // Global layer: packaged defaults, then [global].
    let mut global: HashMap<String, String> =
        GLOBAL_DEFAULTS.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    for (k, v) in ini.defaults.to_map() {
        global.insert(k, v);
    }
    if let Some(g) = ini.section("global") {
        for (k, v) in &g.items {
            global.insert(k.clone(), v.clone());
        }
    }

    // Raw stanza maps (DEFAULT merged), excluding global/default.
    let stanza_names: Vec<String> =
        ini.sections.iter().filter(|s| s.name != "global" && s.name != "default").map(|s| s.name.clone()).collect();
    let mut raw: Vec<(String, HashMap<String, String>)> = Vec::new();
    let mut seen = HashSet::new();
    for s in &ini.sections {
        if s.name == "global" || s.name == "default" || !seen.insert(s.name.clone()) {
            continue;
        }
        raw.push((s.name.clone(), ini.section_map(s)));
    }

    // Stanza inheritance: a stanza whose name (as a regex) matches another
    // stanza's name at its start contributes tokens and unset keys to it.
    let mut inherit: HashMap<String, Vec<String>> = HashMap::new();
    for a in &stanza_names {
        for b in &stanza_names {
            if a != b && pyre_prefix_match(a, b) {
                inherit.entry(b.clone()).or_default().push(a.clone());
            }
        }
    }
    let raw_by_name: HashMap<String, HashMap<String, String>> = raw.iter().cloned().collect();

    let mut stanzas: Vec<StanzaConf> = Vec::new();
    for (name, map) in &raw {
        if let Some(only) = &opts.sample {
            if only != name {
                log::info!("Skipping sample '{}' because of command line override", name);
                continue;
            }
        }
        let mut items: Vec<(String, String)> = map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        items.sort();
        let mut tokens = parse_tokens(name, &items);
        let mut explicit: HashSet<String> = items.iter().map(|(k, _)| k.clone()).collect();
        if let Some(parents) = inherit.get(name) {
            let mut next_index = tokens.iter().map(|t| t.index + 1).max().unwrap_or(0);
            for parent in parents {
                if let Some(pmap) = raw_by_name.get(parent) {
                    let mut pitems: Vec<(String, String)> = pmap.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                    pitems.sort();
                    for mut t in parse_tokens(parent, &pitems) {
                        t.index = next_index;
                        next_index += 1;
                        tokens.push(t);
                    }
                    for (k, v) in pmap {
                        if !k.contains("token") && !map.contains_key(k) {
                            items.push((k.clone(), v.clone()));
                            explicit.insert(k.clone());
                        }
                    }
                }
            }
        }
        let mut settings = global.clone();
        let mut host_token: Option<(String, String)> = None;
        for (k, v) in &items {
            if k.starts_with("token.") {
                continue;
            }
            if k == "host.token" {
                host_token.get_or_insert_with(|| (String::new(), String::new())).0 = v.clone();
                continue;
            }
            if k == "host.replacement" {
                host_token.get_or_insert_with(|| (String::new(), String::new())).1 = v.clone();
                continue;
            }
            settings.insert(k.clone(), v.clone());
        }
        if let Some((t, r)) = &host_token {
            if t.is_empty() || r.is_empty() {
                log::error!("host token in stanza '{}' needs both host.token and host.replacement", name);
                host_token = None;
            }
        }
        if parse_bool(settings.get("disabled").map(String::as_str).unwrap_or("false")) {
            log::info!("Sample '{}' is marked disabled.", name);
            continue;
        }
        let sample_dir = resolve_sample_dir(settings.get("sampleDir"), &conf_dir, &default_sample_dir);
        stanzas.push(StanzaConf {
            stanza: name.clone(),
            name: name.clone(),
            file_path: None,
            sample_dir,
            settings,
            tokens,
            host_token,
            explicit,
        });
    }

    // Match stanzas to sample files.
    let mut matched: Vec<StanzaConf> = Vec::new();
    for s in &stanzas {
        let mut files: Vec<PathBuf> = Vec::new();
        if s.sample_dir.is_dir() {
            let mut names: Vec<String> = std::fs::read_dir(&s.sample_dir)?
                .filter_map(|e| e.ok())
                .filter(|e| e.path().is_file())
                .filter_map(|e| e.file_name().into_string().ok())
                .collect();
            names.sort();
            for fname in names {
                if pyre::full_match(&s.stanza, &fname) {
                    files.push(s.sample_dir.join(&fname));
                }
            }
        } else {
            log::warn!("sample directory {} does not exist", s.sample_dir.display());
        }
        if files.is_empty() {
            log::warn!("Sample '{}' in config but no matching files", s.stanza);
            let g = s.generator();
            if g != "default" && g != "replay" {
                matched.push(s.clone());
            }
            continue;
        }
        for f in files {
            let mut c = s.clone();
            c.name = f.file_name().and_then(|n| n.to_str()).unwrap_or(&s.stanza).to_string();
            c.file_path = Some(f);
            matched.push(c);
        }
    }

    // Flatten: for each file keep the most specific stanza(s) and fold the
    // others' explicit settings and tokens into them.
    let mut masters: Vec<StanzaConf> = Vec::new();
    for (i, s) in matched.iter().enumerate() {
        let Some(path) = &s.file_path else {
            masters.push(s.clone());
            continue;
        };
        let exact = s.name == s.stanza;
        let mut dominated = false;
        if !exact {
            for (j, o) in matched.iter().enumerate() {
                if i == j || o.file_path.as_ref() != Some(path) || o.stanza == s.stanza {
                    continue;
                }
                if o.stanza.len() > s.stanza.len() || o.name == o.stanza {
                    dominated = true;
                    break;
                }
            }
        }
        if dominated {
            continue;
        }
        let mut m = s.clone();
        for o in matched.iter().rev() {
            if o.file_path.as_ref() != Some(path) || o.stanza == s.stanza {
                continue;
            }
            for (k, v) in &o.settings {
                if NON_FLATTEN_KEYS.contains(&k.as_str()) || !o.is_explicit(k) {
                    continue;
                }
                let dest_unset = match m.settings.get(k) {
                    None => true,
                    Some(cur) => global.get(k) == Some(cur) && !m.is_explicit(k),
                };
                if dest_unset && global.get(k) != Some(v) {
                    m.settings.insert(k.clone(), v.clone());
                }
            }
            m.tokens.extend(o.tokens.iter().cloned());
        }
        masters.push(m);
    }

    // Post-processing rules and CLI overrides.
    for s in &mut masters {
        if let Some(c) = opts.override_count {
            s.set("count", &c.to_string());
            s.remove("backfill");
        }
        if let Some(i) = opts.override_interval {
            s.set("interval", &i.to_string());
        }
        if let Some(b) = &opts.override_backfill {
            s.set("backfill", b.trim_start());
        }
        if let Some(e) = &opts.override_end {
            s.set("end", e.trim_start());
        }
        if let Some(o) = &opts.override_output {
            s.set("outputMode", o);
        }
        let pdv = s.get_f64("perDayVolume").filter(|v| *v > 0.0);
        if pdv.is_some() {
            log::info!("Stanza '{}' contains per day volume, using the perdayvolume rater and generator", s.name);
            s.set("rater", "perdayvolume");
            s.set("count", "1");
            s.set("generator", "perdayvolumegenerator");
        } else if s.mode() == "replay" {
            if s.get("earliest").map(|v| v.is_empty()).unwrap_or(true) {
                s.set("earliest", "now");
            }
            if s.get("latest").map(|v| v.is_empty()).unwrap_or(true) {
                s.set("latest", "now");
            }
            s.set("count", "1");
            for k in ["randomizeCount", "hourOfDayRate", "dayOfWeekRate", "minuteOfHourRate"] {
                s.remove(k);
            }
            if !s.is_explicit("interval") {
                s.set("interval", "0");
            }
            s.set("generator", "replay");
            if s.get("end").map(|v| v.is_empty()).unwrap_or(true) {
                s.set("end", "1");
            }
        }
        if s.get("source").map(|v| v.is_empty()).unwrap_or(true) {
            let src = match &s.file_path {
                Some(p) => p.file_name().and_then(|n| n.to_str()).unwrap_or("").to_string(),
                None => s.generator().to_string(),
            };
            s.set("source", &src);
        }
        if parse_bool(s.get("autotimestamp").unwrap_or("false")) {
            log::warn!(
                "autotimestamp is not supported by firebox; declare the timestamp token explicitly (stanza '{}')",
                s.name
            );
        }
        if s.generator() == "jinja" {
            log::warn!("the jinja generator is not supported by firebox (stanza '{}')", s.name);
        }
    }

    Ok(Conf { conf_path, global, samples: masters })
}

fn merge_ini(dst: &mut Ini, src: Ini) {
    for (k, v) in src.defaults.items {
        dst.defaults.set(&k, &v);
    }
    for s in src.sections {
        match dst.section_mut(&s.name) {
            Some(existing) => {
                for (k, v) in s.items {
                    existing.set(&k, &v);
                }
            }
            None => dst.sections.push(s),
        }
    }
}

/// `re.match(a, b)` (anchored at the start only), used for stanza inheritance.
fn pyre_prefix_match(pattern: &str, name: &str) -> bool {
    let anchored = format!("^(?:{})", pyre::translate(pattern));
    match regex::RegexBuilder::new(&anchored).octal(true).build() {
        Ok(re) => re.is_match(name),
        Err(_) => fancy_regex::Regex::new(&anchored).map(|re| re.is_match(name).unwrap_or(false)).unwrap_or(false),
    }
}

fn resolve_sample_dir(setting: Option<&String>, conf_dir: &Path, default: &Path) -> PathBuf {
    match setting {
        Some(v) if !v.trim().is_empty() => {
            let p = PathBuf::from(v.trim());
            if p.is_absolute() {
                p
            } else {
                conf_dir.join(p)
            }
        }
        _ => default.to_path_buf(),
    }
}

/// Collect `token.N.token|replacementType|replacement` into ordered specs,
/// dropping incomplete or invalid ones the way upstream does.
fn parse_tokens(stanza: &str, items: &[(String, String)]) -> Vec<TokenSpec> {
    type Partial = (Option<String>, Option<String>, Option<String>);
    let mut partial: HashMap<usize, Partial> = HashMap::new();
    for (k, v) in items {
        let Some(rest) = k.strip_prefix("token.") else { continue };
        let mut parts = rest.splitn(2, '.');
        let idx = parts.next().and_then(|n| n.parse::<usize>().ok());
        let field = parts.next();
        let (Some(idx), Some(field)) = (idx, field) else {
            log::error!("Could not parse token key '{}' in stanza '{}'", k, stanza);
            continue;
        };
        let entry = partial.entry(idx).or_default();
        match field {
            "token" => entry.0 = Some(v.clone()),
            "replacementType" => {
                if !VALID_REPLACEMENT_TYPES.contains(&v.trim()) {
                    log::error!("Invalid replacementType '{}' for token index '{}' in stanza '{}'", v, idx, stanza);
                    continue;
                }
                entry.1 = Some(v.trim().to_string());
            }
            "replacement" => entry.2 = Some(v.clone()),
            other => log::error!("Could not parse token index '{}' token type '{}' in stanza '{}'", idx, other, stanza),
        }
    }
    let mut indices: Vec<usize> = partial.keys().copied().collect();
    indices.sort_unstable();
    let mut out = Vec::new();
    for idx in indices {
        let (t, rt, r) = partial.remove(&idx).unwrap();
        match (t, rt, r) {
            (Some(token), Some(replacement_type), Some(replacement)) => {
                out.push(TokenSpec { index: idx, token, replacement_type, replacement })
            }
            _ => log::error!("Token at index {} invalid in stanza '{}'", idx, stanza),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn pack(dir: &Path, conf: &str, samples: &[(&str, &str)]) -> PathBuf {
        fs::create_dir_all(dir.join("default")).unwrap();
        fs::create_dir_all(dir.join("samples")).unwrap();
        fs::write(dir.join("default/eventgen.conf"), conf).unwrap();
        for (name, body) in samples {
            fs::write(dir.join("samples").join(name), body).unwrap();
        }
        dir.join("default/eventgen.conf")
    }

    #[test]
    fn layers_defaults_and_matches_files() {
        let tmp = tempdir();
        let conf = pack(
            &tmp,
            "[global]\ninterval = 5\n[web.sample]\ncount = 3\ntoken.0.token = x\ntoken.0.replacementType = static\ntoken.0.replacement = y\ntoken.1.token = bad\ntoken.1.replacementType = nope\ntoken.1.replacement = z\n",
            &[("web.sample", "a\nb\n")],
        );
        let c = load(&conf, &LoadOptions::default()).unwrap();
        assert_eq!(c.samples.len(), 1);
        let s = &c.samples[0];
        assert_eq!(s.name, "web.sample");
        assert_eq!(s.get("interval"), Some("5"));
        assert_eq!(s.get("count"), Some("3"));
        assert_eq!(s.get("index"), Some("main"));
        assert_eq!(s.get("source"), Some("web.sample"));
        assert_eq!(s.tokens.len(), 1);
        assert_eq!(s.tokens[0].replacement, "y");
        assert_eq!(s.sample_dir, tmp.join("samples"));
    }

    #[test]
    fn stanza_regex_matches_multiple_files_and_drops_unmatched() {
        let tmp = tempdir();
        let conf = pack(
            &tmp,
            "[.*\\.log]\ncount = 1\n[nothing]\ncount = 1\n[wind]\ngenerator = windbag\n",
            &[("a.log", "x\n"), ("b.log", "y\n"), ("c.txt", "z\n")],
        );
        let c = load(&conf, &LoadOptions::default()).unwrap();
        let names: Vec<&str> = c.samples.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, vec!["a.log", "b.log", "wind"]);
    }

    #[test]
    fn flatten_prefers_exact_stanza_and_inherits_tokens() {
        let tmp = tempdir();
        let conf = pack(
            &tmp,
            "[.*]\ncount = 7\ntoken.0.token = g\ntoken.0.replacementType = static\ntoken.0.replacement = G\n[web.sample]\ninterval = 9\ntoken.0.token = w\ntoken.0.replacementType = static\ntoken.0.replacement = W\n",
            &[("web.sample", "a\n")],
        );
        let c = load(&conf, &LoadOptions::default()).unwrap();
        assert_eq!(c.samples.len(), 1);
        let s = &c.samples[0];
        assert_eq!(s.stanza, "web.sample");
        assert_eq!(s.get("count"), Some("7"));
        assert_eq!(s.get("interval"), Some("9"));
        // upstream appends the wildcard stanza's tokens once via stanza
        // inheritance and again via file flattening; we keep that behaviour
        let reps: Vec<&str> = s.tokens.iter().map(|t| t.replacement.as_str()).collect();
        assert_eq!(reps, vec!["W", "G", "G"]);
    }

    #[test]
    fn replay_and_perdayvolume_rules() {
        let tmp = tempdir();
        let conf = pack(
            &tmp,
            "[r.sample]\nmode = replay\nrandomizeCount = 0.2\n[p.sample]\nperDayVolume = 2\n",
            &[("r.sample", "a\n"), ("p.sample", "b\n")],
        );
        let c = load(&conf, &LoadOptions::default()).unwrap();
        let r = c.samples.iter().find(|s| s.name == "r.sample").unwrap();
        assert_eq!(r.get("generator"), Some("replay"));
        assert_eq!(r.get("count"), Some("1"));
        assert_eq!(r.get("interval"), Some("0"));
        assert_eq!(r.get("end"), Some("1"));
        assert_eq!(r.get("randomizeCount"), None);
        let p = c.samples.iter().find(|s| s.name == "p.sample").unwrap();
        assert_eq!(p.get("generator"), Some("perdayvolumegenerator"));
        assert_eq!(p.get("rater"), Some("perdayvolume"));
    }

    #[test]
    fn cli_overrides_apply() {
        let tmp = tempdir();
        let conf = pack(&tmp, "[a.sample]\ncount = 1\nbackfill = -1h\n", &[("a.sample", "x\n")]);
        let opts = LoadOptions {
            override_count: Some(5),
            override_interval: Some(2),
            override_end: Some("3".into()),
            override_output: Some("devnull".into()),
            ..Default::default()
        };
        let c = load(&conf, &opts).unwrap();
        let s = &c.samples[0];
        assert_eq!(s.get("count"), Some("5"));
        assert_eq!(s.get("backfill"), None);
        assert_eq!(s.get("interval"), Some("2"));
        assert_eq!(s.get("end"), Some("3"));
        assert_eq!(s.get("outputMode"), Some("devnull"));
    }

    fn tempdir() -> PathBuf {
        let base = std::env::temp_dir().join(format!("firebox-conf-{}-{}", std::process::id(), rand_suffix()));
        fs::create_dir_all(&base).unwrap();
        base
    }

    fn rand_suffix() -> u64 {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let t = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
        t ^ N.fetch_add(1, Ordering::Relaxed)
    }
}
