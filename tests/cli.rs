//! End-to-end tests that drive the built binary.

use std::io::Read;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_firebox"))
}

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/packs")
}

fn tmpdir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("firebox-it-{}-{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// Rewrite a pack conf the way the Stoker agent does: stoker output, absolute
/// sampleDir, fixed interval/count.
fn rewrite(pack: &Path, dst: &Path, count: u32, extra: &[(&str, &str)]) -> PathBuf {
    let text = std::fs::read_to_string(pack.join("default/eventgen.conf")).unwrap();
    let mut out = String::new();
    let mut in_stanza = false;
    for line in text.lines() {
        let t = line.trim();
        if t.starts_with('[') {
            if in_stanza {
                out.push_str(&stanza_tail(pack, count, extra));
            }
            in_stanza = true;
            out.push_str(line);
            out.push('\n');
            continue;
        }
        let key = t.split('=').next().unwrap_or("").trim();
        if in_stanza && ["outputMode", "sampleDir", "interval", "count", "randomizeCount"].contains(&key) {
            continue;
        }
        if in_stanza && extra.iter().any(|(k, _)| *k == key) {
            continue;
        }
        out.push_str(line);
        out.push('\n');
    }
    if in_stanza {
        out.push_str(&stanza_tail(pack, count, extra));
    }
    let conf = dst.join("eventgen.conf");
    std::fs::write(&conf, out).unwrap();
    conf
}

fn stanza_tail(pack: &Path, count: u32, extra: &[(&str, &str)]) -> String {
    let mut s = format!(
        "outputMode = stoker\nsampleDir = {}\ninterval = 1\ncount = {}\n",
        pack.join("samples").display(),
        count
    );
    for (k, v) in extra {
        s.push_str(&format!("{} = {}\n", k, v));
    }
    s
}

struct Sink {
    lines: Arc<Mutex<Vec<String>>>,
    path: PathBuf,
}

impl Sink {
    fn start(path: PathBuf) -> Sink {
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path).unwrap();
        listener.set_nonblocking(true).unwrap();
        let lines = Arc::new(Mutex::new(Vec::new()));
        let l2 = lines.clone();
        std::thread::spawn(move || {
            let mut handles = Vec::new();
            let started = std::time::Instant::now();
            loop {
                match listener.accept() {
                    Ok((mut conn, _)) => {
                        let lines = l2.clone();
                        handles.push(std::thread::spawn(move || {
                            let mut buf = String::new();
                            let _ = conn.read_to_string(&mut buf);
                            let mut g = lines.lock().unwrap();
                            g.extend(buf.lines().map(str::to_string));
                        }));
                    }
                    Err(_) => {
                        if started.elapsed() > Duration::from_secs(60) {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(20));
                    }
                }
            }
        });
        Sink { lines, path }
    }

    fn events(&self) -> Vec<serde_json::Value> {
        std::thread::sleep(Duration::from_millis(300));
        self.lines.lock().unwrap().iter().map(|l| serde_json::from_str(l).unwrap()).collect()
    }
}

impl Drop for Sink {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn run(conf: &Path, cwd: &Path, sock: &Path, secs: f64, extra: &[&str]) -> std::process::Output {
    Command::new(bin())
        .arg("-v")
        .arg("generate")
        .arg(conf)
        .arg("--duration")
        .arg(secs.to_string())
        .args(extra)
        .env("STOKER_OUTPUT_SOCKET", sock)
        .current_dir(cwd)
        .output()
        .unwrap()
}

#[test]
fn version_and_validate() {
    let out = Command::new(bin()).arg("version").output().unwrap();
    assert!(out.status.success());
    assert!(String::from_utf8_lossy(&out.stdout).starts_with("firebox "));
    let out =
        Command::new(bin()).arg("validate").arg(fixtures().join("web-access/default/eventgen.conf")).output().unwrap();
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "{}", text);
    assert!(text.contains("web_access.sample"));
    assert!(text.contains("token.2: HTTP/1"));
}

#[test]
fn flatline_over_stoker_socket() {
    let dir = tmpdir("flatline");
    let pack = fixtures().join("flatline");
    let conf = rewrite(&pack, &dir, 100, &[]);
    let sock = dir.join("out.sock");
    let sink = Sink::start(sock.clone());
    let started = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;
    let out = run(&conf, &pack, &sock, 2.2, &[]);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let events = sink.events();
    assert!(events.len() >= 200 && events.len() <= 300, "{}", events.len());
    let e = &events[0];
    assert_eq!(e["index"], "main");
    assert_eq!(e["sourcetype"], "eventgen");
    assert_eq!(e["host"], "127.0.0.1");
    assert_eq!(e["source"], "flatline.sample");
    for ev in &events {
        let t = ev["time"].as_i64().unwrap();
        assert!(t >= started - 2 && t <= started + 5);
        let text = ev["event"].as_str().unwrap();
        let stamp = &text[text.find(|c: char| c.is_ascii_digit()).unwrap()..][..19];
        assert!(!stamp.starts_with("2025-"), "{}", text);
    }
}

#[test]
fn stdout_output_and_end_executions() {
    // flatline has no rate maps or jitter, so 2 executions x 7 is exact
    let pack = fixtures().join("flatline");
    let out = Command::new(bin())
        .args(["generate", "--stdout", "-c", "7", "-i", "1", "-e", "2"])
        .arg(pack.join("default/eventgen.conf"))
        .current_dir(&pack)
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let lines: Vec<&str> = std::str::from_utf8(&out.stdout).unwrap().lines().collect();
    assert_eq!(lines.len(), 14);
    for l in lines {
        assert!(l.contains("T") && !l.contains("2020-"), "{}", l);
    }
}

#[test]
fn seed_makes_output_reproducible() {
    let pack = fixtures().join("web-access");
    let one = |seed: &str| {
        Command::new(bin())
            .args(["generate", "--stdout", "-c", "20", "-i", "1", "-e", "1", "--threads", "1", "--seed", seed])
            .arg(pack.join("default/eventgen.conf"))
            .current_dir(&pack)
            .output()
            .unwrap()
            .stdout
    };
    let a = one("42");
    let b = one("42");
    let c = one("43");
    // timestamps differ between runs by wall clock, so compare the IP column
    let ips = |o: &[u8]| -> Vec<String> {
        std::str::from_utf8(o).unwrap().lines().map(|l| l.split(' ').next().unwrap().to_string()).collect()
    };
    assert_eq!(ips(&a), ips(&b));
    assert_ne!(ips(&a), ips(&c));
}

#[test]
fn file_output_rotates() {
    let dir = tmpdir("fileout");
    let pack = fixtures().join("flatline");
    let target = dir.join("out.log");
    let text = std::fs::read_to_string(pack.join("default/eventgen.conf")).unwrap();
    let conf = dir.join("eventgen.conf");
    std::fs::write(
        &conf,
        format!(
            "{}\noutputMode = file\nfileName = {}\nfileMaxBytes = 3000\nfileBackupFiles = 2\nsampleDir = {}\n",
            text,
            target.display(),
            pack.join("samples").display()
        ),
    )
    .unwrap();
    let out = Command::new(bin())
        .args(["generate", "-c", "200", "-i", "1", "-e", "1"])
        .arg(&conf)
        .current_dir(&pack)
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    assert!(target.exists());
    assert!(dir.join("out.log.1").exists(), "rotation did not happen");
    let total: usize = ["out.log", "out.log.1", "out.log.2"]
        .iter()
        .filter_map(|n| std::fs::read_to_string(dir.join(n)).ok())
        .map(|t| t.lines().count())
        .sum();
    assert!(total <= 200 && total > 20, "{}", total);
}

#[test]
fn sigterm_exits_cleanly() {
    let dir = tmpdir("sigterm");
    let pack = fixtures().join("flatline");
    let conf = rewrite(&pack, &dir, 50, &[]);
    let sock = dir.join("out.sock");
    let _sink = Sink::start(sock.clone());
    let mut child = Command::new(bin())
        .args(["generate"])
        .arg(&conf)
        .env("STOKER_OUTPUT_SOCKET", &sock)
        .current_dir(&pack)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(1200));
    let pid = child.id();
    let _ = Command::new("kill").args(["-TERM", &pid.to_string()]).status();
    let started = std::time::Instant::now();
    let status = loop {
        if let Some(s) = child.try_wait().unwrap() {
            break s;
        }
        assert!(started.elapsed() < Duration::from_secs(5), "did not exit after SIGTERM");
        std::thread::sleep(Duration::from_millis(50));
    };
    assert!(status.success());
}

#[test]
fn missing_socket_is_a_fatal_error() {
    let dir = tmpdir("nosock");
    let pack = fixtures().join("flatline");
    let conf = rewrite(&pack, &dir, 10, &[]);
    let out = run(&conf, &pack, &dir.join("absent.sock"), 1.0, &[]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("cannot connect"));
}

#[test]
fn hec_envelope_applies_overrides_and_defaults() {
    let dir = tmpdir("hecenv");
    let pack = fixtures().join("flatline");
    let conf = rewrite(&pack, &dir, 20, &[]);
    let sock = dir.join("out.sock");
    let sink = Sink::start(sock.clone());
    let out = Command::new(bin())
        .args(["-v", "generate"])
        .arg(&conf)
        .args(["--duration", "1.2", "--envelope", "hec"])
        .env("STOKER_OUTPUT_SOCKET", &sock)
        .env(
            "STOKER_ENVELOPE_META",
            r#"{"overrides": {"index": "loadtest"}, "defaults": {"source": "ignored-engine-has-one", "host": "x"}}"#,
        )
        .current_dir(&pack)
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let events = sink.events();
    assert!(!events.is_empty());
    for e in &events {
        assert_eq!(e["index"], "loadtest"); // override beats the engine's `main`
        assert_eq!(e["host"], "127.0.0.1"); // engine value beats the default
        assert_eq!(e["source"], "flatline.sample");
        assert_eq!(e["sourcetype"], "eventgen");
        assert!(e["time"].is_i64());
        assert!(e.get("fields").is_none());
        assert_eq!(e.as_object().unwrap().len(), 6);
    }
}

// --------------------------------------------------------------------------
// Identity rotation, through the real binary
// --------------------------------------------------------------------------

/// A pack whose sample is the customer's own three-event journey: the same
/// user logs in and logs out, with a second user in between.
fn rotation_pack(dir: &Path, token_extra: &[(&str, &str)]) -> PathBuf {
    std::fs::create_dir_all(dir.join("default")).unwrap();
    std::fs::create_dir_all(dir.join("samples")).unwrap();
    std::fs::write(
        dir.join("samples/sessions.sample"),
        "user=483920 action=login\nuser=771045 action=register\nuser=483920 action=logout\n",
    )
    .unwrap();
    let mut conf = String::from(
        "[sessions.sample]\nmode = sample\ninterval = 1\ncount = -1\nearliest = -1s\nlatest = now\n\
         outputMode = stdout\nend = 3\n\
         token.0.token = user=(\\d+)\ntoken.0.replacementType = rotate\ntoken.0.replacement = keep\n",
    );
    for (k, v) in token_extra {
        conf.push_str(&format!("{k} = {v}\n"));
    }
    let path = dir.join("default/eventgen.conf");
    std::fs::write(&path, conf).unwrap();
    path
}

fn generate(conf: &Path, cwd: &Path, env: &[(&str, &str)]) -> Vec<String> {
    let mut cmd = Command::new(bin());
    cmd.args(["generate"]).arg(conf).current_dir(cwd);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let out = cmd.output().unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    String::from_utf8_lossy(&out.stdout).lines().filter(|l| !l.trim().is_empty()).map(str::to_string).collect()
}

fn field(line: &str, key: &str) -> String {
    line.split_whitespace()
        .find_map(|p| p.strip_prefix(key))
        .unwrap_or_else(|| panic!("no {key} in {line}"))
        .to_string()
}

#[test]
fn rotation_gives_each_replay_a_new_user_who_still_joins() {
    // The whole point: a stable stand-in replays one user logging in ten
    // thousand times, which makes anything that groups by user meaningless.
    // Each pass must be a NEW user whose login and logout still belong
    // together.
    let dir = tmpdir("rotate-pass");
    let conf = rotation_pack(&dir, &[]);
    let lines = generate(&conf, &dir, &[]);
    assert_eq!(lines.len(), 9, "3 events x 3 intervals: {lines:?}");

    // The originals must be gone: rotation replaces them.
    assert!(!lines.iter().any(|l| l.contains("483920") || l.contains("771045")), "{lines:?}");

    let mut journeys: std::collections::HashMap<String, Vec<String>> = std::collections::HashMap::new();
    for line in &lines {
        journeys.entry(field(line, "user=")).or_default().push(field(line, "action="));
    }
    // 3 passes x 2 users per pass, every identity distinct.
    assert_eq!(journeys.len(), 6, "{journeys:?}");
    let mut shapes: Vec<Vec<String>> = journeys.into_values().collect();
    for shape in shapes.iter_mut() {
        shape.sort();
    }
    shapes.sort();
    assert_eq!(
        shapes,
        vec![
            vec!["login".to_string(), "logout".to_string()],
            vec!["login".to_string(), "logout".to_string()],
            vec!["login".to_string(), "logout".to_string()],
            vec!["register".to_string()],
            vec!["register".to_string()],
            vec!["register".to_string()],
        ],
        "each pass must be one complete journey plus one registration"
    );
}

#[test]
fn rotation_keeps_the_field_format() {
    // The customer's extractions, numeric comparisons and dashboards all
    // depend on the id still looking like an id.
    let dir = tmpdir("rotate-format");
    let conf = rotation_pack(&dir, &[]);
    for line in generate(&conf, &dir, &[]) {
        let user = field(&line, "user=");
        assert_eq!(user.len(), 6, "{line}");
        assert!(user.chars().all(|c| c.is_ascii_digit()), "{line}");
    }
}

#[test]
fn rotation_widens_on_request() {
    let dir = tmpdir("rotate-widen");
    let conf = rotation_pack(&dir, &[("token.0.replacement", "digits(15)")]);
    for line in generate(&conf, &dir, &[]) {
        let user = field(&line, "user=");
        assert_eq!(user.len(), 15, "{line}");
        assert!(user.chars().all(|c| c.is_ascii_digit()), "{line}");
    }
}

#[test]
fn two_worker_slots_never_mint_the_same_identity() {
    // The property that matters at the customer's 58 slots: every worker walks
    // the same ordinals, and no identity may appear in two of them.
    let dir = tmpdir("rotate-slots");
    let conf = rotation_pack(&dir, &[("token.0.replacement", "digits(12)")]);
    let mut seen: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
    for slot in 0..4u32 {
        for line in generate(&conf, &dir, &[("STOKER_ROTATE_WORKERS", "4"), ("STOKER_ROTATE_SLOT", &slot.to_string())])
        {
            let user = field(&line, "user=");
            if let Some(other) = seen.insert(user.clone(), slot) {
                assert_eq!(other, slot, "identity {user} minted by slot {other} and slot {slot}");
            }
        }
    }
    assert!(seen.len() >= 20, "only {} identities over 4 slots", seen.len());
}

#[test]
fn a_restart_resumes_on_a_pass_boundary() {
    // Resuming mid-pass would re-mint an identity already sent to Splunk, so
    // the base is rounded up to the next pass.
    let dir = tmpdir("rotate-resume");
    let conf = rotation_pack(&dir, &[]);
    let first = generate(&conf, &dir, &[]);
    // 9 events over a 3-line sample is 3 whole passes, so resuming at 9 must
    // give identities none of the first run used.
    let after = generate(&conf, &dir, &[("STOKER_ROTATE_BASE", "9")]);
    let before: std::collections::HashSet<String> = first.iter().map(|l| field(l, "user=")).collect();
    for line in &after {
        assert!(!before.contains(&field(line, "user=")), "{line} repeats a sent identity");
    }
    // A base inside a pass is rounded UP rather than splitting the pass, so
    // every base in (9, 12] starts at the same place: ordinal 12, pass 4.
    let ids = |base: &str| {
        generate(&conf, &dir, &[("STOKER_ROTATE_BASE", base)])
            .iter()
            .map(|l| field(l, "user="))
            .collect::<std::collections::HashSet<_>>()
    };
    assert_eq!(ids("10"), ids("12"), "a mid-pass base must round up to the next pass");
    assert_eq!(ids("10"), ids("11"));
    assert_ne!(ids("10"), ids("9"), "and 9 is already a boundary, so it is a pass earlier");
}

#[test]
fn aligned_rotation_joins_two_unrelated_samples() {
    // The cross-sourcetype option. Two packs that share neither a sample, a
    // line count nor a fleet must still land on the same identity for the same
    // stand-in, or a correlation search that joins web to auth returns nothing.
    let web = tmpdir("rotate-aligned-web");
    let auth = tmpdir("rotate-aligned-auth");
    std::fs::create_dir_all(web.join("default")).unwrap();
    std::fs::create_dir_all(web.join("samples")).unwrap();
    std::fs::create_dir_all(auth.join("default")).unwrap();
    std::fs::create_dir_all(auth.join("samples")).unwrap();
    std::fs::write(web.join("samples/web.sample"), "client=483920 path=/home\n").unwrap();
    std::fs::write(
        auth.join("samples/auth.sample"),
        "src=10.0.0.1 client=483920 result=ok\nsrc=10.0.0.2 client=999111 result=fail\n",
    )
    .unwrap();
    let stanza = |name: &str, workers: &str| {
        format!(
            "[{name}]\nmode = sample\ninterval = 1\ncount = -1\nearliest = now\nlatest = now\n\
             outputMode = stdout\nend = 1\nrotate.scope = window\nrotate.period = 3600\n\
             token.0.token = client=(\\d+)\ntoken.0.replacementType = rotate\n\
             token.0.replacement = keep\n{workers}"
        )
    };
    std::fs::write(web.join("default/eventgen.conf"), stanza("web.sample", "")).unwrap();
    std::fs::write(auth.join("default/eventgen.conf"), stanza("auth.sample", "")).unwrap();

    // Different fleets as well as different samples, since the aligned scope
    // must ignore the slot entirely.
    let from_web = generate(
        &web.join("default/eventgen.conf"),
        &web,
        &[("STOKER_ROTATE_WORKERS", "4"), ("STOKER_ROTATE_SLOT", "3")],
    );
    let from_auth = generate(&auth.join("default/eventgen.conf"), &auth, &[]);
    let web_id = field(&from_web[0], "client=");
    let auth_ids: Vec<String> = from_auth.iter().map(|l| field(l, "client=")).collect();
    assert!(auth_ids.contains(&web_id), "{web_id} not in {auth_ids:?}: the join would fail");
    assert_ne!(web_id, "483920", "the original must still be replaced");
    assert_eq!(web_id.len(), 6);
}
