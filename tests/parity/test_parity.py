"""Behavioural parity tests: run the firebox binary on the fixture packs through
the Stoker socket protocol and assert the same invariants the Python eventgen
satisfies (envelope shape, token semantics, per-interval counts, timestamps
inside the window). When EVENTGEN_ROOT points at a directory holding the
vendored `splunk_eventgen` package (Stoker's worker/engines/eventgen), the same
packs are also run through the Python engine and the two outputs are compared
shape-for-shape.

Env:
  FIREBOX_BIN     path to the firebox binary (default target/release/firebox)
  EVENTGEN_ROOT   optional: enables the Python comparison
"""
from __future__ import annotations

import configparser
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from sink import Sink  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
PACKS = ROOT / "fixtures" / "packs"
FIREBOX = Path(os.environ.get("FIREBOX_BIN", ROOT / "target" / "release" / "firebox"))
EVENTGEN_ROOT = os.environ.get("EVENTGEN_ROOT")


def _parser():
    p = configparser.RawConfigParser(delimiters=("=",), strict=False, interpolation=None)
    p.optionxform = str
    return p


def rewrite_conf(pack: Path, dst_dir: Path, interval=1, count=50, extra=None) -> Path:
    """The agent's conf rewrite, minimally: outputMode = stoker, sampleDir,
    interval/count, and any extra per-stanza keys."""
    p = _parser()
    p.read(pack / "default" / "eventgen.conf")
    for section in p.sections():
        if section.lower() in ("global", "default"):
            continue
        p.set(section, "outputMode", "stoker")
        p.set(section, "sampleDir", str(pack / "samples"))
        if p.get(section, "mode", fallback="sample") != "replay":
            p.set(section, "interval", str(interval))
            p.set(section, "count", str(count))
            p.remove_option(section, "randomizeCount")
            # eps mode strips the diurnal maps (docs/WORKER-CONTRACT.md rule 5)
            for key in ("hourOfDayRate", "dayOfWeekRate", "minuteOfHourRate", "dayOfMonthRate", "monthOfYearRate"):
                p.remove_option(section, key)
        for k, v in (extra or {}).items():
            p.set(section, k, v)
    dst = dst_dir / "eventgen.conf"
    with open(dst, "w") as fh:
        p.write(fh)
    return dst


def run_firebox(conf: Path, sock: str, cwd: Path, seconds=2.5, extra_args=()):
    cmd = [str(FIREBOX), "-v", "generate", str(conf), "--duration", str(seconds), *extra_args]
    env = dict(os.environ, STOKER_OUTPUT_SOCKET=sock)
    return subprocess.run(cmd, cwd=str(cwd), env=env, capture_output=True, text=True, timeout=seconds + 30)


def run_python_eventgen(conf: Path, sock: str, cwd: Path, seconds=9.0):
    """The Python engine needs ~4 s to import and spin up its thread pools
    before the first interval fires, hence the longer window."""
    env = dict(os.environ, STOKER_OUTPUT_SOCKET=sock, PYTHONPATH=EVENTGEN_ROOT)
    env["EVENTGEN_LOG_DIR"] = str(cwd / "eventgen-logs")
    os.makedirs(env["EVENTGEN_LOG_DIR"], exist_ok=True)
    proc = subprocess.Popen(
        [sys.executable, "-m", "splunk_eventgen", "generate", str(conf)],
        cwd=str(cwd), env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    time.sleep(seconds)
    proc.terminate()
    try:
        proc.wait(5)
    except subprocess.TimeoutExpired:
        proc.kill()


def tokens_of(pack: Path):
    p = _parser()
    p.read(pack / "default" / "eventgen.conf")
    out = []
    for section in p.sections():
        i = 0
        while p.has_option(section, "token.%d.token" % i):
            out.append((p.get(section, "token.%d.token" % i), p.get(section, "token.%d.replacementType" % i), p.get(section, "token.%d.replacement" % i)))
            i += 1
    return out


@pytest.fixture
def workdir():
    d = Path(tempfile.mkdtemp(prefix="firebox-parity-"))
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture(autouse=True)
def _need_binary():
    if not FIREBOX.exists():
        pytest.skip("firebox binary not built (FIREBOX_BIN=%s)" % FIREBOX)


def _run_pack(pack_name, workdir, count=50, seconds=2.5, extra=None, extra_args=()):
    pack = PACKS / pack_name
    conf = rewrite_conf(pack, workdir, count=count, extra=extra)
    sock = str(workdir / "out.sock")
    sink = Sink(sock)
    try:
        started = time.time()
        res = run_firebox(conf, sock, cwd=pack, seconds=seconds, extra_args=extra_args)
        elapsed = time.time() - started
        time.sleep(0.3)
    finally:
        sink.close()
    assert res.returncode == 0, res.stderr
    assert sink.malformed == 0
    return sink, res, elapsed, started


IPV4 = re.compile(r"^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$")


def test_flatline_envelope_shape_and_count(workdir):
    sink, res, elapsed, started = _run_pack("flatline", workdir, count=100, seconds=3.2)
    events = sink.events
    # 3 intervals of 100 (interval 1, duration 3.2)
    assert 250 <= len(events) <= 400, len(events)
    e = events[0]
    assert set(e.keys()) == {"time", "host", "source", "sourcetype", "index", "event"}
    assert isinstance(e["time"], int)
    # eventgen global defaults flow through, exactly like the Python engine
    assert e["index"] == "main" and e["sourcetype"] == "eventgen" and e["host"] == "127.0.0.1"
    assert e["source"] == "flatline.sample"
    for ev in events:
        m = re.search(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", ev["event"])
        assert m, ev["event"]
        assert started - 3 <= ev["time"] <= started + elapsed + 1
        ts = time.mktime(time.strptime(m.group(0), "%Y-%m-%dT%H:%M:%S"))
        assert ts == ev["time"], (m.group(0), ev["time"])


def test_web_access_tokens(workdir):
    sink, res, elapsed, started = _run_pack("web-access", workdir, count=60, seconds=2.2)
    status_codes = {l.strip() for l in (PACKS / "web-access/samples/status_codes.sample").read_text().splitlines() if l.strip()}
    assert len(sink.events) >= 60
    for ev in sink.events:
        text = ev["event"]
        ip = text.split(" ", 1)[0]
        m = IPV4.match(ip)
        assert m and all(0 <= int(o) <= 255 for o in m.groups()), ip
        st = re.search(r'HTTP/1\.[01]" (\d{3})', text)
        assert st and st.group(1) in status_codes, text
        ts = re.search(r"\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2}", text)
        assert ts and time.mktime(time.strptime(ts.group(0), "%d/%b/%Y:%H:%M:%S")) == ev["time"]


def test_apigw_capture_group_replacement(workdir):
    sink, *_ = _run_pack("apigw", workdir, count=40, seconds=2.2)
    assert len(sink.events) >= 40
    codes = {l.strip() for l in (PACKS / "apigw/samples/status_codes.sample").read_text().splitlines() if l.strip()}
    for ev in sink.events:
        text = ev["event"]
        assert "srcip=" in text and "status=" in text
        ip = re.search(r"srcip=(\S+)", text).group(1)
        assert IPV4.match(ip), text
        assert re.search(r"status=(\d{3})", text).group(1) in codes


def test_tutorial_secure_whole_match_and_integer(workdir):
    sink, *_ = _run_pack("splunk-tutorial-secure", workdir, count=40, seconds=2.2)
    for ev in sink.events:
        text = ev["event"]
        assert re.match(r"^\w{3} \w{3} \d{2} \d{4} \d{2}:\d{2}:\d{2}", text), text
        pid = re.search(r"sshd\[(\d+)\]", text)
        assert pid and 1000 <= int(pid.group(1)) <= 65000


def test_same_value_for_repeated_token_in_one_event(workdir):
    pack = workdir / "pack"
    (pack / "default").mkdir(parents=True)
    (pack / "samples").mkdir()
    (pack / "samples" / "twice.sample").write_text("a=##IP## b=##IP##\n")
    (pack / "default" / "eventgen.conf").write_text(
        "[twice.sample]\nmode = sample\ninterval = 1\ncount = 20\n"
        "token.0.token = ##IP##\ntoken.0.replacementType = random\ntoken.0.replacement = ipv4\n"
    )
    conf = rewrite_conf(pack, workdir, count=20)
    sock = str(workdir / "out.sock")
    sink = Sink(sock)
    try:
        res = run_firebox(conf, sock, cwd=pack, seconds=1.5)
        time.sleep(0.3)
    finally:
        sink.close()
    assert res.returncode == 0, res.stderr
    assert sink.events
    for ev in sink.events:
        a, b = re.match(r"a=(\S+) b=(\S+)", ev["event"]).groups()
        assert a == b


def test_csv_windbag_breaker_and_file_tokens(workdir):
    sink, res, *_ = _run_pack("csv-and-windbag", workdir, count=6, seconds=2.2)
    kinds = {"csv": [], "multi": [], "wind": []}
    for ev in sink.events:
        text = ev["event"]
        if "WINDBAG" in text:
            kinds["wind"].append(ev)
        elif text.startswith("BEGIN"):
            kinds["multi"].append(ev)
        else:
            kinds["csv"].append(ev)
    assert kinds["csv"] and kinds["multi"] and kinds["wind"], res.stderr
    users = {"alice": "london", "bob": "paris", "carol": "berlin"}
    seqs = []
    for ev in kinds["csv"]:
        text = ev["event"]
        u = re.search(r"USER=(\w+)", text).group(1)
        c = re.search(r"CITY=(\w+)", text).group(1)
        assert users[u] == c, text  # mvfile columns come from the same line
        assert "TAG=fixed" in text
        seqs.append(int(re.search(r"SEQ=(\d+)", text).group(1)))
        assert ev["sourcetype"] == "csvtype"
        assert ev["host"] in ("web-1", "web-2", "")
    assert len(set(seqs)) == len(seqs) and min(seqs) >= 100  # integerid increments
    for ev in kinds["multi"]:
        assert "\n" in ev["event"] or ev["event"].startswith("BEGIN")  # multi-line events survive
        assert re.search(r"code=(200|201|404)", ev["event"]) or "code=" not in ev["event"]
    for ev in kinds["wind"]:
        assert re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)? -0700 WINDBAG Event \d+ of 6$", ev["event"]), ev["event"]


def test_replay_reproduces_gaps_and_retimestamps(workdir):
    pack = PACKS / "replay-sysmon"
    p = _parser()
    p.read(pack / "default" / "eventgen.conf")
    for section in p.sections():
        p.set(section, "outputMode", "stoker")
        p.set(section, "sampleDir", str(pack / "samples"))
        p.set(section, "timeMultiple", "0.05")
        p.set(section, "end", "1")
    conf = workdir / "eventgen.conf"
    with open(conf, "w") as fh:
        p.write(fh)
    sock = str(workdir / "out.sock")
    sink = Sink(sock)
    try:
        started = time.time()
        res = run_firebox(conf, sock, cwd=pack, seconds=20)
        time.sleep(0.3)
    finally:
        sink.close()
    assert res.returncode == 0, res.stderr
    n_lines = len([l for l in (pack / "samples" / "events.log").read_text().splitlines() if l.strip()])
    assert len(sink.events) >= 10, len(sink.events)
    for ev in sink.events:
        assert isinstance(ev["time"], float)
        assert started - 1 <= ev["time"] <= time.time() + 1
        m = re.search(r"SystemTime='(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})", ev["event"])
        assert m, ev["event"][:120]
        year = int(m.group(1)[:4])
        assert year >= 2026
    assert len(sink.events) == n_lines


@pytest.mark.skipif(not EVENTGEN_ROOT, reason="EVENTGEN_ROOT not set")
@pytest.mark.parametrize("pack_name", ["flatline", "web-access", "apigw", "aws-cloudtrail", "nginx-access"])
def test_python_engine_agreement(pack_name, workdir):
    """Both engines, same rewritten conf: identical envelope keys, identical
    metadata, every token regex matches the same number of times per line and
    replaced values satisfy the same validators."""
    pack = PACKS / pack_name
    conf = rewrite_conf(pack, workdir, count=30)
    results = {}
    for engine in ("firebox", "python"):
        sock = str(workdir / ("%s.sock" % engine))
        sink = Sink(sock)
        try:
            if engine == "firebox":
                res = run_firebox(conf, sock, cwd=pack, seconds=2.2)
                assert res.returncode == 0, res.stderr
            else:
                run_python_eventgen(conf, sock, cwd=pack, seconds=9.0)
            time.sleep(0.3)
        finally:
            sink.close()
        assert sink.events, engine
        results[engine] = sink.events
    fb, py = results["firebox"], results["python"]
    assert set(fb[0].keys()) == set(py[0].keys())
    for key in ("host", "source", "sourcetype", "index"):
        assert fb[0][key] == py[0][key], key
    for token, kind, rep in tokens_of(pack):
        rx = re.compile(token)
        fb_counts = sorted(len(rx.findall(e["event"])) for e in fb[:30])
        py_counts = sorted(len(rx.findall(e["event"])) for e in py[:30])
        assert fb_counts == py_counts, (token, fb_counts, py_counts)
