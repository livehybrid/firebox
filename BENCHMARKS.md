# Benchmarks

Measured 2026-09-26 on the AIOS development box: an Intel i7-4770HQ (4 cores,
8 threads, 2.2 GHz) inside a container on a host whose load average sat
between 8 and 10 from other work throughout. Treat every number as a lower
bound with roughly +/-20% noise; the ratios between engines are the point.

Pack: `web-access` (NCSA combined access log, 160 sample lines of ~190 bytes,
three tokens: a timestamp, a random IPv4 and a weighted status code read from
a file). Same rewritten conf for both engines.

## Raw engine throughput (no agent, output discarded)

`firebox bench fixtures/packs/web-access/default/eventgen.conf --seconds 4`:

| threads | events/s | MB/s | per thread |
|---|---|---|---|
| 1 | 104,884 | 19.8 | 104,884 |
| 2 | 204,252 | 38.5 | 102,126 |
| 4 | ~252,000 | 47.4 | ~63,000 |
| 8 | 135k to 213k (noise) | | |

Scaling is linear up to the two idle physical cores; beyond that the box has
no spare cores to give. The vendored Python `splunk_eventgen` templates the
same pack at roughly 700 events/s per process on this machine (it is single
threaded by design), so one Firebox thread is about 150 times faster and the
whole process 300 to 400 times.

Two things made the difference between the first cut (64k/s on one thread, no
scaling) and the numbers above:

- the `regex` crate shares one cache pool per `Regex` and only the owning
  thread gets the lock-free path, so each worker now holds its own clone and a
  reusable `CaptureLocations`;
- musl's allocator serialises multi-threaded allocation, so the token path no
  longer allocates per event (values go into per-thread scratch buffers, event
  strings are pooled, local time is cached per second).

## Through the Stoker worker agent (the real pipeline)

`tools/bench_engines.py` in the Stoker repo: the standalone agent
(`python -m stoker_agent`, EPS mode, 20 s per run) with each engine, delivering
to `tools/hec_sink.py`. "Delivered" is what the sink received.

| engine | target eps | delivered eps | achieved | agent+engine CPU (cores) |
|---|---|---|---|---|
| python eventgen | 2,000 | 690 | 35% | 0.94 |
| python eventgen | 5,000 | 288 | 6% | 0.96 |
| python eventgen | 10,000+ | 0 in 20 s (first batch not finished) | 0% | 0.94 |
| firebox | 2,000 | 2,000 | 100% | 0.71 |
| firebox | 5,000 | 4,731 | 95% | 1.04 |
| firebox | 10,000 | 4,351 | 44% | 0.98 |
| firebox | 20,000 | 3,482 | 17% | 0.74 |

Firebox delivers the target exactly up to the agent's own ceiling of about
4.5 to 4.7k events/s per worker on this box, which is where the Python
*agent* (not the engine) runs out of one core: its socket reader parses each
envelope, paces it through the token bucket and re-serialises it for HEC. The
Python engine never reaches the target at all: at 2,000 eps it manages a third,
and at 10,000 eps it cannot template its first interval's batch within 20 s.

## With the HEC-line envelope (`--envelope hec`)

The agent's per-event work was the wall above, so Firebox gained an envelope
mode in which it applies the agent's metadata rules itself and emits final
HEC objects; the agent's reader then only paces each line and forwards the
bytes (Stoker's `STOKER_FAST_ENVELOPE`, on by default with Firebox). Same
harness, same pack:

| engine | target eps | delivered eps | achieved | agent+engine CPU (cores) |
|---|---|---|---|---|
| firebox, hec envelope | 5,000 | 4,998 | 100% | 0.83 |
| firebox, hec envelope | 10,000 | 10,000 | 100% | 1.07 |
| firebox, hec envelope | 20,000 | 11,895 | 59% | 1.02 |
| firebox, hec envelope | 50,000 | 15,343 | 31% | 1.28 |
| firebox, hec envelope | 100,000 | 11,060 | 11% | 1.01 |

Exact delivery now holds to 10,000 eps per worker and the ceiling sits around
15,000, three times the classic envelope and over twenty times the Python
engine's 690. What remains is the Python agent's per-line pacing and queue
work (about 65 microseconds per event on one core) and its HEC senders; going
further means batching the token bucket and the queue hand-off inside the
agent, not anything in the engine.

## Socket connections

The agent reads each engine connection on its own thread. With one connection
per generator thread (eight readers) the readers starve the four HEC sender
threads for the GIL and the HEC queue pins at its 5,000 cap:

| connections | delivered eps at a 20k target | agent CPU |
|---|---|---|
| per thread (8) | 1,885 | 94% |
| single shared | 4,781 | 100% |

Hence `--connections single` is the default.
