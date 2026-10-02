# Firebox

A fast, multi-threaded, `eventgen.conf`-compatible event generator. Firebox is
a drop-in replacement for `python -m splunk_eventgen generate <conf>`: it reads
the same configuration, loads the same sample files, applies the same token
replacements and raters, and writes to the same outputs, but it is a single
static binary that uses every core instead of one GIL-bound Python thread.

It exists for [Stoker](https://github.com/livehybrid/stoker), whose worker
image vendored Splunk eventgen 7.2.1 and topped out at a few thousand events
per second per worker. Firebox speaks Stoker's agent protocol natively
(`outputMode = stoker`), so the agent does not change at all.

## Quick start

```sh
# generate to stdout, one stanza's worth every second, three times
firebox generate path/to/default/eventgen.conf --stdout -i 1 -c 100 -e 3

# what would run, which tokens compile, how many lines each sample has
firebox validate path/to/default/eventgen.conf

# raw templating throughput of a pack (timers off, output discarded)
firebox bench path/to/default/eventgen.conf --seconds 5
```

`firebox generate` accepts eventgen's flags (`-s`, `-c`, `-i`, `-b`, `-e`,
`--devnull`, `--modinput`, `--generators`) plus a few of its own:

| Flag | Meaning |
|---|---|
| `--threads N` / `FIREBOX_THREADS` | generator threads (default: all cores; cgroup limits are respected) |
| `--seed N` / `FIREBOX_SEED` | reproducible output |
| `--duration S` | stop after S seconds |
| `--stats S` | log throughput every S seconds |
| `--socket PATH` / `STOKER_OUTPUT_SOCKET` | the Stoker agent socket for `outputMode = stoker` |
| `--connections single\|per-thread` | one shared socket connection (default) or one per thread |
| `-v`, `-vv` | info / debug logging on stderr (default: errors only) |

## What is supported

- **Config**: `[global]`, `DEFAULT`, stanza inheritance and file flattening,
  `sampleDir` resolution, `disabled`, stanza names as regexes over the sample
  directory, app-directory confs (`default/` + `local/`).
- **Generators**: `default`, `perdayvolumegenerator`, `replay`, `windbag`,
  `counter`.
- **Raters**: `config` (count, `randomizeCount`, `hourOfDayRate`,
  `dayOfWeekRate`, `minuteOfHourRate`, `dayOfMonthRate`, `monthOfYearRate`),
  `perdayvolume` (with the token-size correction and interval carry),
  `backfill` (which actually works: every past interval is generated with its
  own window and rate-map time).
- **Tokens**: `static`, `timestamp`, `replaytimestamp`, `random` and `rated`
  (`ipv4`, `ipv6`, `mac`, `guid`, `integer[a:b]`, `float[a:b]`, `string(n)`,
  `hex(n)`, `list[...]`), `file`, `mvfile` (`path:column`), `seqfile`,
  `integerid` (with state files), `host.token`/`host.replacement`.
- **Identity rotation** (`rotate`, a firebox and Stoker extension, not in
  upstream eventgen): mints a new identity for every pass over the sample, in
  the format of the value it replaces, so a replayed journey is a different
  user each time round instead of one user logging in ten thousand times.
  `rotate.scope = pass` (default) counts passes and is collision-free by
  construction, including across worker slots; `rotate.scope = window` with
  `rotate.period` counts clock windows instead, so the same id lines up across
  sourcetypes, packs and runs at the cost of a birthday-rate collision chance.
  `token.N.replacement` is `keep`, `digits(N)`, `hex(N)` or `guid`.
- **Samples**: `sampletype = raw` with the default or a custom `breaker`,
  `sampletype = csv` with per-row `index`/`host`/`source`/`sourcetype`,
  `randomizeEvents`, `bundlelines`, `sequentialTimestamp`, `extendIndexes`,
  `timezone`, `earliest`/`latest` (Splunk relative time incl. snap-to),
  `delay`, `end` (count of executions or a time), `timeMultiple`.
- **Outputs**: `stoker` (Stoker agent socket), `stdout`, `file` (with
  rotation), `devnull`, `counter`, `modinput`, `httpevent` (Splunk HEC,
  round-robin or mirror, gzip; HTTPS with the `tls` feature).

Not supported (logged at start): the `jinja` generator, `autotimestamp`,
`splunkstream`/`tcpout`/`udpout`/`syslogout`/`s2s`/`spool`/`awss3`/`scsout`
outputs, `backfillSearch`, the Splunk-embedded controller. See
[COMPAT.md](COMPAT.md) for the exact semantics and the handful of deliberate
differences from upstream.

## How it is fast

- Every regex and every strftime format is compiled once at load.
- Each worker thread has its own random generator and scratch buffers; no
  locks on the hot path. Output is encoded and written once per batch.
- An interval's work is split into chunks that any thread can run, so a single
  stanza uses the whole machine.
- Local time is cached per second; the envelope JSON is written directly into
  the output buffer.
- A stalled sink applies backpressure through blocking writes, and intervals
  that could not be scheduled in time are skipped rather than replayed late,
  which is the behaviour a pacing agent wants.

`firebox bench` prints events per second for a pack on this machine. Numbers
from the development box are in [BENCHMARKS.md](BENCHMARKS.md).

## Building

```sh
cargo build --release                     # host target, plain HTTP HEC
cargo build --release --features tls      # + HTTPS (needs a C compiler for ring)
make build TARGET=x86_64-unknown-linux-musl
```

The musl targets link with rustc's bundled `rust-lld` (see
`.cargo/config.toml`), so the pure-Rust feature set builds on a machine with no
C toolchain at all. CI (`.github/workflows/ci.yml`) runs fmt, clippy, the unit
and integration tests and the Python parity harness, then cross-builds static
amd64 and arm64 binaries, publishes a multi-arch `scratch` image to
`ghcr.io/livehybrid/firebox` (cosign-signed) and attaches tarballs to tagged
releases.

`Dockerfile` builds from source for any platform buildx asks for (pure Rust
by default; `--build-arg FEATURES=tls` needs a C compiler with musl headers
for the target, which CI provides with zig); it is the same stage Stoker's
worker image uses to embed the binary.

## Tests

```sh
cargo test                                          # unit + CLI integration tests
FIREBOX_BIN=target/release/firebox python3 -m pytest tests/parity   # protocol/semantics harness
EVENTGEN_ROOT=/path/to/stoker/worker/engines/eventgen \
  python3 -m pytest tests/parity                      # + side-by-side with the Python engine
```

The parity harness drives the binary through the Stoker socket protocol on the
packs in `fixtures/packs` and checks the invariants the Python engine
satisfies. With `EVENTGEN_ROOT` set it also runs the vendored Python eventgen
on the same rewritten conf and compares envelope shape, metadata and per-line
token match counts.

## Licence

Apache-2.0. Firebox reimplements the documented behaviour of
[splunk/eventgen](https://github.com/splunk/eventgen) (Apache-2.0); no
eventgen or gogen code is included.
