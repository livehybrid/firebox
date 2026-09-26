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
