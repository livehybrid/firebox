# Compatibility with splunk_eventgen 7.2.1

Firebox aims for behavioural parity with `splunk_eventgen generate` as vendored
in Stoker (`worker/engines/eventgen`). This file lists the semantics that were
verified against the upstream source, the deliberate deviations (all of them
fixes of upstream defects, each documented), and what is not implemented.

## Verified parity

**Config layering.** Packaged `[global]` defaults (`count = -1`, `interval =
60`, `earliest`/`latest = now`, `index = main`, `sourcetype = eventgen`, `host =
127.0.0.1`, `outputMode = modinput`, ...), then `[global]` from the conf, then
`DEFAULT`, then the stanza. Keys are case-sensitive; `=` or `:` delimits; `#`
and `;` start comments; indented lines continue a value. A stanza whose name
matches another stanza's name at its start contributes tokens (appended) and
unset keys. Stanza names are regexes that must match a whole file name in the
sample directory; several files may match one stanza, several stanzas one
file, in which case the exact/longest stanza wins and the others' explicit
settings and tokens are folded in (tokens are appended even when they were
already inherited, exactly as upstream does).

**Post-processing.** `perDayVolume` switches to the perdayvolume rater and
generator with `count = 1`. `mode = replay` forces `count = 1`, clears
`randomizeCount` and the hour/day/minute maps, defaults `interval` to 0 and
`end` to 1. `source` defaults to the sample file name.

**Token replacement.** Per event and per token: find all matches, compute one
value, write it into every match. Group 1 is replaced when the pattern has a
group and it participated; otherwise the whole match. Any error path ("Start
integer greater than end", "Unknown replacement", missing file, bad column)
leaves the text untouched. `integer[a:b]` is inclusive; `float[a:b]` rounds
to the number of decimals of the start value and prints like Python's
`str(float)` (`42.0`); `string(n)` yields n URL-safe characters; `hex(n)` is
upper-case; `ipv6` groups are unpadded lower-case hex; `mac` is six
zero-padded lower-case pairs; `guid` is a lower-case UUID4. `file`/`mvfile`
pick a random line (`seqfile` picks in order); `path:N` selects column N of a
comma-split line and, within one event, every token reading the same file
sees the same picked line. `integerid` counts up from its replacement and
persists `state.<quoted token>` in the sample directory. `rotate` has no
upstream equivalent and is documented below. `timestamp` needs
`lt >= et` and a format with at least one conversion. `rated` multiplies an
integer/float by the hour-of-day and day-of-week maps (Sunday = 0).

**Identity rotation (`rotate`).** A firebox and Stoker extension, so a pack
using it is not portable to upstream eventgen, which drops an unknown
`replacementType` and emits the matched text verbatim. That degradation is
deliberately safe: the pack holds pseudonymised stand-ins already, so an engine
that has never heard of `rotate` emits the stable stand-in rather than leaking
anything.

Unlike every other replacement type, `rotate` is evaluated **per match**, not
once per event: the replacement is a function of the matched text, so two
different ids in one event rotate to two different values.

Under the default `rotate.scope = pass` the identity is a counter,
`(pass x workers + slot) x D + k`, where `pass` is how many times the stanza's
event ordinal has wrapped the sample, `D` the number of distinct sample values
sharing that format class and `k` the value's position in that class. Read right
to left that is a mixed-radix number, so it is injective: two passes and two
worker slots can never mint the same identity. `D` and `k` come from tables
built from the sample at load, walked line by line in file order, then token by
token in conf order, then match by match left to right, so every worker and
both engines derive them identically and nothing has to ship with the pack.
A rotating stanza also selects its sample line by ordinal rather than
restarting at line 0 each interval, so a pass is never left part-finished.
`STOKER_ROTATE_WORKERS`, `STOKER_ROTATE_SLOT` and `STOKER_ROTATE_BASE` carry
the fleet position and the resume point; a resume is rounded up to the next
pass boundary, because resuming mid-pass would re-mint an identity already
sent. Exhausting the format's space is logged once and then wraps rather than
failing the run.

Under `rotate.scope = window` (with `rotate.period`, default 60 seconds) the
identity is instead `mix(stand-in, epoch / period) mod space(F)`, a function of
the value and the clock alone. Every pack, run and worker slot computes the same
value, so an id joins across sourcetypes, which a pass counter cannot do because
the ordinal and the line count are private to one stanza. The trade is that a
hash collides where a counter cannot, at the birthday rate for the field's
format, and that cardinality becomes `distinct values x duration / period`. The
mixer is FNV-1a-64 then splitmix64's finaliser, pinned by
`fixtures/format_vectors.json`, which both engines are tested against.

**Timestamps.** Naive local time (`datetime.now()`); `timezone = +HHMM` gives
`utcnow() + offset`. The default generator picks a random whole second in
`[earliest, latest]` per event (or evenly spaced with `sequentialTimestamp`)
and stamps `_time` with it as an integer epoch. strftime is the C locale
(`%a %b %d %e %H %I %M %S %f %p %j %y %Y %m %z %Z %s %% %T %F %R %D %c ...`);
`%z`/`%Z` are empty (naive datetime), unknown conversions are emitted
literally.

**Raters.** `count * f` rounded half-to-even where `f` is `randomizeCount`
jitter (uniform within +/- half the value) times the rate maps at "now"
(missing keys leave the factor alone). `count = -1` with the default generator
means the whole sample. perdayvolume: `GB * 1024^3 / (86400 / interval)` bytes
per interval times `f`, carried over while smaller than one event, filled in
order from line 1 and corrected by the pre/post token replacement size ratio.

**Generators.** default: sequential lines from line 1 every interval (not a
rolling cursor), `randomizeEvents` random lines, `bundlelines` whole-file
copies. windbag: `"<datetime> -0700 WINDBAG Event i of n"` spaced over the
window, `count = -1` -> 60. replay: gaps from consecutive parsed timestamps
times `timeMultiple`, wall-clock stamping at emission, timestamp tokens
rewritten to the event time, `_time` as a float epoch, one pass then `end`.

**Samples.** Raw lines (blank lines dropped, `\r\n` normalised, latin-1
fallback for non-UTF-8), custom `breaker` regexes keeping the breaker with each
event, CSV via a `_raw` column with optional metadata columns (index for the
whole batch comes from the first row, as upstream).

**Outputs.** `stoker`: `{"time","host","source","sourcetype","index","event"}`
NDJSON with `null` for unset metadata, blocking writes, fatal on failure.
`stdout`/`file`/`devnull`: the raw text, one per line. `file`: append,
rotate at `fileMaxBytes` keeping `fileBackupFiles` numbered backups.
`httpevent`: `httpeventServers`, `httpeventOutputMode`,
`httpeventMaxPayloadSize`, `httpeventAllowFailureCount`.

## Deliberate deviations (fixes)

| Area | Upstream | Firebox |
|---|---|---|
| `%s` in a timestamp format | `str(epoch).rstrip("0")` mangles epochs ending in zero | full epoch seconds |
| `rated` integer tokens | look up `hourOfDayRate[str(datetime)]` (KeyError, never applied) | look up by hour like the float branch |
| `timestamp` tokens in replay mode | the first event's time sticks to every later event (`sample.timestamp` never reset) | each event uses its own replay time |
| replay `_time` | naive local time treated as UTC (off by the TZ offset) | correct local epoch |
| `@q` snap | `floor(month/3.3+1)*3` (wrong for every month) | the calendar quarter start |
| relative `earliest`/`latest` | parsed once and frozen as an offset from first use | re-evaluated each interval |
| `backfill` rater | non-functional in the vendored tree (0 events) | generates every past interval, then continues live |
| replay after a backfill | first live event stamped at the backfill horizon | stamped now |
| perdayvolume size ratio | recomputed every interval | computed once at load |
| perdayvolume byte counting | Python `len()` (code points) | bytes (identical for ASCII) |
| `list[...]` non-string items | crash | stringified |
| `generatorWorkers` | thread/process pool size, default 1 | ignored; `--threads` / all cores |
| Late intervals | the timer drifts and can burst | skipped, never bursted (a stalled sink backpressures, stale jobs are dropped) |
| An app directory as the conf | samples resolved against the app's parent | `<app>/samples` |
| `outputMode = modinput` | Splunk modular input stream | the same XML stream on stdout |

## Not implemented

`generator = jinja` (a Jinja2 template engine with eventgen's time
extensions), `autotimestamp`/`autotimestamps`, `backfillSearch`,
`splitSample`, `useOutputQueue`/`outputWorkers`/`maxIntervalsBeforeFlush`
(meaningless here: every worker writes its own batches), the Splunk-embedded
REST controller, and the `splunkstream`, `tcpout`, `udpout`, `syslogout`,
`s2s`, `spool`, `awss3`, `scsout`, `metric_httpevent` outputs. A stanza that
needs one of these logs a warning (or a hard error for an unknown generator)
so the gap is visible instead of silent.

## Regex dialect

Token patterns are Python `re` syntax. They run on the `regex` crate after a
small translation (`\Z` -> `\z`, escaped punctuation, a `{` that is not a
quantifier, `{,n}`, `(?P=name)`, Python-only flags) and fall back to
`fancy-regex` for lookaround and backreferences. `firebox validate` shows
which engine each token compiled on.
