# Watchtower — Built-in Log Viewer (Elasticsearch-optional)

**Date:** 2026-06-28
**Status:** Approved design, pre-implementation
**Author:** brainstorming session

## Problem

Today Watchtower is a pure forwarding sidecar: logs arrive over gRPC, the pipeline
fans each batch out to configured **sinks** (`elasticsearch` / `opensearch` via HTTP
bulk, or `watchtower` forward via gRPC). To actually *read* the logs you must stand
up Elasticsearch/OpenSearch or an upstream aggregator. There is no local storage and
no UI.

We want Watchtower to be **self-sufficient as a minimal log stack**: with no sink
configured at all, it should retain logs in a local store and serve a web UI to
search and visualize them. Elasticsearch/OpenSearch/forward stay fully intact — they
just stop being mandatory.

## Goals

- Run Watchtower with **zero downstream sinks** and still keep + view logs.
- A built-in **web viewer** served by Watchtower itself: live tail, per-record
  detail, severity color-coding, and filtering by text/severity/service/time.
- One binary serves both **dev** (ephemeral, in-memory, zero setup) and **small
  production** (durable file, retention) — durability is just the DB path.
- Preserve the project's lean ethos: no JS framework, no build step, minimal new
  dependencies, small binary.

## Non-goals (YAGNI for v1)

FTS5 / advanced query language, charts/aggregations, alerting, multiple indices,
disk-size-based retention, Server-Sent Events, authentication beyond optional HTTP
basic auth, multi-tenant access control.

## Decisions (resolved during brainstorming)

| Decision | Choice |
|---|---|
| Primary use case | Both dev + small production; configurable durability, lightweight default |
| Storage backend | Embedded **SQLite** via `rusqlite` (`bundled` feature) |
| DB sharing model | A **single shared connection behind a `Mutex`** for both writes and reads |
| Default `db_path` | `:memory:` (ephemeral); set a file path for durability |
| Viewer transport | Dedicated HTTP server on its **own port** (`:9092`), separate from health/metrics (`:9091`) |
| UI richness | **Full** — live tail + detail drawer + severity color-coding + filters |
| Live tail mechanism | **Polling** (`/api/logs?after_id=...` every ~1.5s) — no SSE |
| Text search (v1) | `LIKE '%...%'` on `body` (no FTS5) |
| Security | Opt-in; **bind `127.0.0.1` by default**; **optional** basic auth |

## Architecture

A new top-level config section `viewer` ties everything together. When
`viewer.enabled` is true, startup does two things:

1. Registers an internal **store sink** (SQLite) that participates in the pipeline
   fan-out exactly like any other sink.
2. Starts the **viewer HTTP server** on `viewer.listen_addr`, serving the UI and a
   JSON query API that reads from the same database.

```
                gRPC LogBatch
   App ───────────────────────────► Watchtower
                                       │
                                  ┌────▼─────┐
                                  │ Pipeline │ (unchanged: buffer + fan-out)
                                  └────┬─────┘
              ┌──────────────┬─────────┼───────────────┐
              ▼              ▼         ▼                ▼
        Elasticsearch   OpenSearch  forward      ┌───────────┐
         (optional)     (optional)  (optional)   │ StoreSink │  ◄── new
                                                 │  (SQLite) │
                                                 └─────┬─────┘
                                                       │ shared Mutex<Connection>
                                                 ┌─────▼──────────┐
                                                 │ Viewer HTTP     │  ◄── new
                                                 │ :9092           │
                                                 │  GET /          │ (embedded UI)
                                                 │  GET /api/logs  │
                                                 │  GET /api/services │
                                                 └────────────────┘
```

### Why a single shared connection behind a `Mutex`

SQLite `:memory:` databases are **private to one connection** — a separate reader
connection cannot see what a writer connection wrote. To make the in-memory (default)
path work without `cache=shared` gymnastics, and to keep file-mode dead simple, the
store owns **one** `rusqlite::Connection` wrapped in a `tokio::sync::Mutex`, shared
(via `Arc`) between the store sink (writes) and the viewer HTTP handlers (reads).
`rusqlite` is synchronous, so every DB operation runs inside `tokio::task::spawn_blocking`.

Throughput is adequate for a sidecar: writes are batched (one transaction per flush),
queries are infrequent and fast against indexed columns. If a single connection ever
becomes a bottleneck we can revisit (WAL + separate reader pool for file DBs), but
that is explicitly out of scope for v1.

## Configuration

```yaml
viewer:
  enabled: false                    # opt-in; default off
  listen_addr: "127.0.0.1:9092"     # localhost by default for safety
  db_path: ":memory:"               # ephemeral default; file path => durable
  retention:
    max_records: 1000000            # trim oldest beyond this many rows
    max_age: "7d"                   # delete rows older than this
  auth:                             # optional HTTP basic auth; omit to disable
    username: "admin"
    password: "changeme"
```

Config struct additions (`src/config/mod.rs`):

- `Config.viewer: ViewerConfig`
- `ViewerConfig { enabled: bool, listen_addr: String, db_path: String, retention: RetentionConfig, auth: Option<BasicAuthConfig> }`
- `RetentionConfig { max_records: u64, max_age: Duration }`
- `BasicAuthConfig { username: String, password: String }`
- Defaults: `enabled=false`, `listen_addr="127.0.0.1:9092"`, `db_path=":memory:"`,
  `max_records=1_000_000`, `max_age=7d`, `auth=None`.

### Duration parser extension

The current `humantime_serde` parser supports only `ms` / `s` / `m`. `max_age` needs
hours and days, so extend `parse_duration` to also accept `h` (×3600s) and `d`
(×86400s). Order the suffix checks so `ms` is still matched before `m`/`s`.

### Validation change

`Config::validate()` currently allows zero sinks silently. New rule:

- If `sinks` is empty **and** `viewer.enabled` is false → return a validation error
  ("no sinks configured and viewer disabled: logs would be discarded").
- If `sinks` is empty **and** `viewer.enabled` is true → OK (viewer is the sink).

`watchtower.example.yaml`, `README.md`, and `docs/configuration.md` updated to
document the viewer and a "no Elasticsearch" minimal config.

## Storage — SQLite store sink

New file `src/sink/store.rs` implementing the existing `Sink` trait.

Schema (created on open if absent):

```sql
CREATE TABLE IF NOT EXISTS logs (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_nanos      INTEGER NOT NULL,
  severity      INTEGER NOT NULL,
  service_name  TEXT,
  host_name     TEXT,
  instance_id   TEXT,
  body          TEXT,
  trace_id      TEXT,
  span_id       TEXT,
  attributes    TEXT,        -- JSON object (same shape as the ES document)
  schema_url    TEXT
);
CREATE INDEX IF NOT EXISTS idx_logs_ts        ON logs(ts_nanos);
CREATE INDEX IF NOT EXISTS idx_logs_severity  ON logs(severity);
CREATE INDEX IF NOT EXISTS idx_logs_service   ON logs(service_name);
```

For a file DB, set `PRAGMA journal_mode=WAL` and `PRAGMA synchronous=NORMAL` on open.

Write path: `Sink::send(batch)` inserts all records in a single transaction inside
`spawn_blocking`, holding the shared connection mutex. `id` (autoincrement) doubles as
the keyset-pagination cursor and the live-tail "after" cursor.

### Shared record→JSON helper

`record_to_json` / `any_value_to_json` / timestamp conversion currently live in
`src/sink/elastic.rs`. Extract them into a shared module (e.g. `src/sink/encode.rs`)
and have both the elastic sink and the store sink use it. `attributes` is stored as
the JSON object; `ts_nanos` derives from the proto timestamp (seconds×1e9 + nanos).
No behavior change to the existing ES document shape.

## Query API (viewer HTTP server)

New file `src/viewer/mod.rs` (hyper http1, same style as `src/health.rs`). Routes:

- `GET /` — serves the embedded UI page (`Content-Type: text/html`).
- `GET /api/logs` — JSON query. Parameters (all optional):
  - `q` — substring match on `body` (`LIKE '%q%'`).
  - `min_severity` — severity name (`INFO`) or int; matches `severity >= value`.
  - `service` — exact `service_name`.
  - `since` / `until` — RFC3339 timestamp or relative (`1h`, `30m`, `2d`).
  - `after_id` — return rows with `id > after_id` ascending (live tail).
  - `before_id` — return rows with `id < before_id` descending (older page).
  - `limit` — default 100, max 1000.
  - Response: `{ "records": [ {full record}... ], "oldest_id": N, "newest_id": M }`.
    The full structured record (attributes, trace/span, resource, schema_url) is
    included so the detail drawer needs no second request.
- `GET /api/services` — `{ "services": ["auth-svc", ...] }` distinct service names for
  the filter dropdown.
- No `/healthz` on the viewer port in v1 — the existing `:9091` health server is
  unchanged and remains the single health/metrics surface.

All DB reads run in `spawn_blocking` under the shared connection mutex. Optional basic
auth is enforced as middleware on every route when `viewer.auth` is set (401 +
`WWW-Authenticate: Basic` on mismatch).

## Viewer UI

A single self-contained `src/viewer/index.html` (HTML + CSS + vanilla JS, **no
framework, no build step, no external CDN**), baked into the binary via
`include_str!`. Features:

- Log table with **severity color-coding** (INFO neutral, WARN amber, ERROR/FATAL
  red, DEBUG/TRACE muted).
- Filter bar: text search, minimum severity, service dropdown (from `/api/services`),
  time range.
- **Live tail**: polls `/api/logs?after_id=<newest_id>&<filters>` every ~1.5s and
  prepends new rows. A **Follow** toggle pauses/resumes polling.
- **Older history**: scrolling/`Load older` issues `before_id` keyset pagination.
- **Detail drawer**: clicking a row opens a side panel rendering the full structured
  record from the already-fetched JSON (attributes, trace_id, span_id, resource,
  schema_url).

No external assets keeps it CSP-friendly and works fully offline inside the sidecar.

## Retention

A background tokio task (interval ~60s) prunes the DB while the viewer is enabled:

- Delete rows where `ts_nanos < now - max_age`.
- Then, if `COUNT(*) > max_records`, delete the lowest `id` rows down to
  `max_records` (keyset).

Runs under the shared connection mutex via `spawn_blocking`. Skipped entirely when the
viewer is disabled.

## Wiring (`src/main.rs`)

- After building the configured `sinks`, if `viewer.enabled`:
  1. Open the SQLite connection (file or `:memory:`), create schema, build the shared
     `Arc<Mutex<Connection>>`.
  2. Construct `StoreSink` over the shared connection and push it onto the `sinks`
     vector before building the pipeline.
  3. Spawn the retention task.
  4. Spawn the viewer HTTP server on `viewer.listen_addr`, wired to the same shared
     connection and the shutdown watch channel.
- Graceful shutdown: viewer server subscribes to the existing `shutdown_tx` watch
  channel, same pattern as the health server.

## Dependencies & build

- Add `rusqlite = { version = "0.32", features = ["bundled"] }` (pin to the latest
  release at implementation time). `bundled` compiles SQLite from source — requires a
  C toolchain (`cc`/`gcc`) at build time. Verify the
  `Dockerfile` build stage provides one (the official `rust` image does; a `-slim`
  base may need `gcc` added). Expected binary growth ~1–1.5 MB.
- No frontend toolchain is introduced.

## Testing

- **Unit (store):** insert a batch → query back; verify filters (`q`, `min_severity`,
  `service`, `since`/`until`); verify `after_id`/`before_id` paging; verify retention
  trims by age and by count.
- **Unit (api):** query-parameter parsing (severity names + ints, relative vs RFC3339
  time, limit clamping).
- **Unit (config):** viewer defaults; validation rejects "zero sinks + viewer off"
  and accepts "zero sinks + viewer on"; duration parser accepts `h`/`d`.
- **Integration:** boot Watchtower with only the viewer enabled, send a `LogBatch`
  over gRPC, assert it appears via `GET /api/logs`; assert basic auth returns 401
  without credentials when configured.
- **Both DB modes:** run the store tests against `:memory:` and a temp file path.

## Files touched / created

| File | Change |
|---|---|
| `src/config/mod.rs` | `ViewerConfig` + nested structs + defaults; duration `h`/`d`; validation rule |
| `src/sink/store.rs` | **new** — SQLite `StoreSink` (schema, batched insert, queries, retention) |
| `src/sink/encode.rs` | **new** — extracted `record_to_json` / `any_value_to_json` / timestamp helpers |
| `src/sink/elastic.rs` | use shared `encode` module instead of local copies |
| `src/sink/mod.rs` | export `store` module |
| `src/viewer/mod.rs` | **new** — hyper HTTP server, routes, basic-auth middleware, query layer |
| `src/viewer/index.html` | **new** — embedded single-page UI |
| `src/lib.rs` | export `viewer` module |
| `src/main.rs` | open DB, register store sink, spawn viewer + retention, shutdown wiring |
| `Cargo.toml` | add `rusqlite` (bundled) |
| `watchtower.example.yaml` | document `viewer` section + no-ES minimal config |
| `README.md` / `docs/configuration.md` | document the viewer, storage, retention, security |
| `Dockerfile` | ensure C toolchain in build stage |
| `tests/integration_test.rs` | viewer end-to-end test |
```
