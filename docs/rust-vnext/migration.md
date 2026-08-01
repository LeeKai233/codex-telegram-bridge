# Rust vNext Migration Gates

The migration is deliberately staged. Python remains the only `getUpdates`
consumer for 9527 until a separate maintenance window explicitly transfers
ownership. Rust may use the 9527 credential for `getMe` and outbound delivery,
but it must not start a second poller for that token.

## Gate 0: inventory

Run the read-only inventory against a copy or the live SQLite database:

```bash
cargo run -q -p codex-telegram-cli -- migration-inspect \
  "$HOME/.local/state/codex-telegram-bridge/state.sqlite3"
```

The command reports only schema version, table names/counts, and whether the
binding is a legacy linked discussion or a Forum binding. It does not modify
the database and does not export Telegram identifiers or message contents.

## Gate 1: compatibility

1. Run `validate`, `probe`, and the checked-in replay fixtures.
2. Start the Rust metrics exporter on `127.0.0.1:9465`; keep Python on
   `127.0.0.1:9464` after its next controlled restart.
3. Compare the shared observability contract and alert rules for one full
   observation window.
4. Exercise approval and artifact ports with no physical Bot configured:
   high-risk execution must be denied, while durable artifacts remain retained
   for a later transfer attempt.

## Gate 2: outbound canary

Use the 411 canary alert Bot for a synthetic, non-production notification. Use
826 only for production alert routing after the canary has passed. Do not put
token values, prompts, chat IDs, or captured updates in replay fixtures or
logs.

## Gate 3: ownership transfer

During a declared maintenance window:

1. Stop Python cleanly and confirm its updater has released 9527.
2. Acquire the Rust process lock and verify the lock path contains only a
   digest, never a token.
3. Start exactly one Rust poller, verify `allowed_updates`, and observe the
   polling-staleness alert and recovery path.
4. Roll back by stopping Rust and starting Python; never run both consumers
   concurrently for one token.

No automatic database rewrite or destructive cleanup is part of these gates.
The Rust SQLite store is a new event-backed schema; a future importer must be
an explicit export/import tool with a backup and a reconciliation report.
