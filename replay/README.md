# Sanitized Replay and Benchmark Contract

`v1.schema.json` defines one event per NDJSON line. It is a deterministic behavioural input shared
by Rust vNext and the existing Bridge's future compatibility adapter. A replay drives only an
in-memory adapter: it must never open a Telegram connection, read a production SQLite database,
start a tmux session, discover a host path, or send a message.

## Sanitization boundary

Fixtures are synthetic by construction. Every identifier has the `replay-` prefix, timestamps are
relative `offset_ms` values, and the schema has no field for text bodies, URLs, paths, credentials,
Telegram identifiers, exception messages, or arbitrary metadata. A producer must reject rather than
redact a record that does not validate against `v1.schema.json`; silent redaction can conceal a
privacy regression in the benchmark corpus.

To derive a fixture from a real incident, first map it into the fixed fields in this schema, replace
all identities with independent `replay-*` values, remove all content, and use relative timing. Do
not commit a captured journal, app-server event, Telegram update, SQLite row, prompt, plan, or log.

## Replay requirements

An implementation accepts an NDJSON fixture, validates every line before processing the first event,
and processes lines ordered by nondecreasing `offset_ms`. It must reject a duplicate invalid record,
a decreasing offset, an event that violates its state machine, or any attempted external side effect.
It may implement a virtual clock; benchmark timing measures host processing time, not the fixture's
simulated elapsed time.

At minimum, implementations support the supplied event kinds and these invariants:

- `ordered_offsets`: offsets never decrease.
- `nonnegative_queue_depth`: a queue depth never becomes negative.
- `terminal_delivery`: every `delivery_attempted` record reaches exactly one terminal
  `delivery_completed` record with the same synthetic thread and attempt.
- `poll_recovers`: a failed polling health state becomes healthy only after a successful
  `poll_completed` record for that role.

The Python adapter and Rust vNext may expose different local command names, but both must support
equivalent arguments: an input fixture, an optional named scenario, a positive repetition count, a
warm-up count, and JSON report output. Their output validates against
`benchmark-report-v1.schema.json`; JSON report output must contain no absolute paths, hostnames,
usernames, environment values, or raw errors.

## Benchmark manifest

`benchmarks/v1.toml` declares the checked-in synthetic scenarios. A runner reads a scenario's
fixture, performs `warmup_repetitions` outside the measurement, replays it `repetitions` times, and
emits exactly one benchmark-report JSON object. A failing invariant produces `outcome: "failed"` and
a bounded `failure_class`, exits nonzero, and does not publish partial performance claims.

The committed fixtures are contract fixtures, not production performance baselines. Report hardware,
runtime version, and command invocation outside the JSON object when comparing machines; do not add
those host-specific values to the fixture or result schema.
