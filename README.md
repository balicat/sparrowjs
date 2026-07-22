# sparrowJS

[![npm](https://img.shields.io/npm/v/%40sparrowflight%2Fjs)](https://www.npmjs.com/package/@sparrowflight/js)
[![tests](https://github.com/balicat/sparrowjs/actions/workflows/test.yml/badge.svg)](https://github.com/balicat/sparrowjs/actions/workflows/test.yml)

**The missing browser client for Apache Arrow Flight** — a browser implementation of
Apache Arrow Flight and Flight SQL over gRPC-web. Works with any Apache Arrow Flight
or Flight SQL server.

> **Status** &nbsp; ✔ on npm: `npm install @sparrowflight/js` &nbsp;·&nbsp; ✔ powers the [live demo at sparrowflight.io](https://sparrowflight.io/demo/js) &nbsp;·&nbsp; 0.x — API may still move
> **Supports** &nbsp; ✔ Flight &nbsp; ✔ Flight SQL &nbsp; ✔ browser &nbsp; ✔ Node (same transport) &nbsp; ✔ gRPC-web
> **Validated against** &nbsp; ✔ Dremio OSS &nbsp; ✔ InfluxDB 3 Core &nbsp; ✔ GizmoSQL (DuckDB) &nbsp; ✔ Sparrow Flight

[![The live demo — ten charts, one Flight call](docs/demo.png)](https://sparrowflight.io/demo/js)

*This is [the live demo](https://sparrowflight.io/demo/js). Your browser opens a
Flight SQL connection to a 136-million-row production server — ten full-history
series, one call, half a second. No REST gateway. No JSON backend. (The second
timing line is one, though: the REST+JSON control group racing on the same page —
1.6× slower, 4.8 MB of JSON against 1.7 MB of Arrow.)*

## Why

Arrow Flight was designed for high-performance analytics. Browsers were left behind —
they never learned gRPC, so most Flight deployments end the same way: a REST gateway
converting Arrow back into JSON before a chart can touch it. sparrowJS removes that
translation layer.

Flight over **gRPC-web**, decoded by **Apache Arrow's JavaScript library**, streaming
record batches directly into the page — no JSON, no REST gateway in between.

The same auth + discovery pattern is validated against **GizmoSQL (DuckDB)**,
**InfluxDB 3 Core**, **Dremio OSS**, and the EnergyScope production server
([Sparrow](https://sparrowflight.io)).

## The API

```js
import { connect } from "@sparrowflight/js";

const client = await connect({
  endpoint: "/flight",
  user: "demo",   // Basic in, Bearer adopted automatically
});

// Flight SQL — the everyday path. One method, two consumption styles:
for await (const batch of client.query("SELECT period, value FROM series_data WHERE …")) {
  chart.append(batch);      // Apache Arrow JS RecordBatch, as it arrives
}
const { table, stats } = await client.query("SELECT …");
// stats: { planMs, firstBatchMs, streamMs, rows, wireBytes, mbitPerSec, … }

// typed builder, metadata, int64 policy:
await client.from("series_data").select("period", "value").limit(1000).query();
client.capabilities();      // GetSqlInfo decoded — vendor, substrait, readOnly…
await client.tables();      // GetTables discovery (the portable path)
await client.query(sql, { bigIntMode: "string" }); // int64 without 2^53 surprises

// raw Flight against any server: client.getFlightInfo(desc) → client.doGet(ticket)

// 1-RTT pull — servers that accept client-made JSON tickets (Sparrow does)
// skip GetFlightInfo entirely: measured 143 ms vs 224 ms for the same
// 10,217-row series over the same wire. Flight at REST's latency floor.
await client.pull(["PET.RWTC.D"], { start: "2020-01-01" });

// …and query() takes the same shortcut BY ITSELF where the server
// advertises the "sql" direct-ticket template (SqlInfo 10100): the whole
// statement rides the ticket, arbitrary SQL at 1 RTT. stats.route tells
// you which path ran; { direct: false } forces the planned 2-RTT path.
const { stats } = await client.query("SELECT …");  // stats.route → "direct"
```

Full surface + design rationale: [docs/api-m1.md](docs/api-m1.md). `connect()` fails
fast on a bad endpoint or credentials, adopts the Bearer, and seeds `capabilities()`
in the same round trip. Every result carries wire stats — the same anatomy
sparrowCLI's `--stats` prints. The live demo runs this exact library.

## Install

```sh
npm install @sparrowflight/js apache-arrow
```

`apache-arrow` is a peer dependency (≥17) — your bundle shares one copy.
The library is 44 kB gzipped, ESM, tree-shakeable, TypeScript types included.

## How it works

```
your browser
    │  sparrowjs
    │  gRPC-web
    ▼
Envoy grpc_web filter          ← config, not code
    │  native gRPC (h2c)
    ▼
any Apache Arrow Flight SQL server
(Sparrow · GizmoSQL/DuckDB · ROAPI/DataFusion · Dremio · InfluxDB 3 · …)
```

- **Transport** — `@connectrpc/connect-web` (fetch + ReadableStream). Browsers can't
  speak gRPC, so a translation layer sits at the edge: any Envoy with the standard
  `grpc_web` filter (or a Traefik `grpcWeb` middleware) in front of your Flight server.
  The one powering the live demo runs nginx → Envoy `grpc_web` → Flight server.
- **Decode** — `FlightData` frames are reassembled into an Arrow IPC stream
  (continuation marker + padded header + padded body + EOS) and handed to
  `apache-arrow`. Arrow columns are exposed as typed arrays after decode.
- **View types** — Arrow JS can't decode `Utf8View`/`BinaryView` (the types
  modern DataFusion servers emit for every parquet-sourced string column), so
  sparrowJS **transcodes them to classic `Utf8`/`Binary` at the IPC layer**
  before Arrow JS sees them. DataFusion/ROAPI servers work as-is — no
  `schema_force_view_types` config, no asking the server's operator. To our
  knowledge no other JS Flight client does this.
- **Compression (0.5.0)** — the browser reads **lz4-compressed Arrow IPC**:
  sparrowJS registers an [lz4js](https://www.npmjs.com/package/lz4js) codec on
  apache-arrow 21.2's `compressionRegistry` (the decode hook from arrow-js
  PR #14) and every 1-RTT ticket declares `accept_compression: ["lz4"]` — the
  server compresses **only** for a client that said it can decode. Negotiated
  end to end and feature-detected: on apache-arrow < 21.2 the codec isn't
  registered, the ticket stays silent, and the server ships plain IPC that any
  Arrow can read. Nothing to configure, no version fence (the peer range stays
  `>=17`). Measured on the demo's 10,217-row pull: **201 KB → 130 KB on the
  wire (1.6×)**; string-heavy pulls compress ~2–3.3×.
- **Auth** — Basic bootstrap, then **Bearer adoption**: many Flight servers
  (GizmoSQL-style) mint a Bearer from your Basic credentials and bind the session to
  it, so the client adopts the token from the response headers — the same silent trick
  the ADBC drivers do.

## The numbers (and the honest part)

We also built the control experiment: the same 136M-row snapshot behind a
conventional REST+JSON API, raced button-against-button
[on the live demo](https://sparrowflight.io/demo/js). Measured July 2026:

| query | Arrow Flight | REST+JSON | winner |
|---|---|---|---|
| 1 series · 10,217 rows (2-RTT SQL) | 345 ms · 201 KB | **240 ms** · 56 KB gz | REST — one round trip beats Flight's two |
| 1 series · same, via `pull()` (1-RTT ticket) | **143 ms** · 201 KB | 149 ms · 56 KB gz | dead heat — `pull()` removes the extra round trip (2026-07-17) |
| 1 series · same, `query()` auto-routed (1-RTT `sql` ticket) | **137 ms** · 201 KB | 149 ms · 56 KB gz | arbitrary SQL at the same floor — no code change (0.4.0) |
| 10 series · 71,979 rows | **588 ms** · 1.7 MB | 850 ms · 4.8 MB JSON | Arrow — the backend's JSON factory becomes the bottleneck |

(Wire sizes in the table are pre-0.5.0 captures. With negotiated lz4 the same
1-RTT pulls now ship **130 KB instead of 201 KB** — REST's gzip edge on the
wire is mostly gone, at the same timing floor.)

Small queries favour REST; the gap flips and grows with payload because the server
stops spending CPU manufacturing JSON. Under load the difference is structural —
16 concurrent clients on the same 2-vCPU box: **Arrow 13.8 req/s (p95 1.4 s) vs
REST 3.1 req/s (p95 8.4 s)**. Full write-up on
[sparrowflight.io/js](https://sparrowflight.io/js).

Streaming is real, not aspirational: the client decodes record batches as they
arrive (`onBatch` callback). On the demo's "watch it stream" button the first chart
paints at ~270 ms — before the entire REST response has landed — and all ten charts
complete by ~900 ms across 36 record batches.

## The landscape (as of July 2026)

Good JavaScript Flight SQL clients exist — targeting **Node**, riding native gRPC,
which browsers cannot speak. Verified July 2026:

| | Intended runtime | Transport | Flight SQL |
|---|---|---|---|
| `apache-arrow` (JS) | browser + Node | — (Arrow IPC decode only; no Flight transport) | — |
| `gizmodata/gizmosql-client-js` | Node ≥ 20 | native gRPC (`@grpc/grpc-js`) | ✅ |
| `lancedb/flight-sql-js-client` | Node (*"currently all testing is done on Node"*) | native gRPC | ✅ (experimental) |
| **`@sparrowflight/js`** | **browser-first** (proven in Node too) | **gRPC-web** | ✅ |

In the browser, the standard answer is still a REST/JSON backend in front of Flight.
sparrowJS implements the transport stack browsers lack — connect-web → gRPC-web →
Flight RPC → Flight SQL → Arrow IPC → typed arrays — so the page talks to the Flight
server itself.

## Run it from source

```sh
npm install
npm run gen        # regenerate Flight/FlightSql stubs from the Apache protos (buf)

# proof script — the full chain in Node using the *browser* transport:
SPARROW_ENDPOINT=http://your-grpc-web-edge:8890 \
SPARROW_USER=user SPARROW_PASS=pass \
node src/m0.mjs "SELECT 42 AS answer"
```

Bundle for a page (this is exactly how the live demo is built):

```sh
npx esbuild src/demo-entry.js --bundle --minify --format=iife \
  --global-name=SparrowJS --target=es2020 --outfile=sparrow-demo.js
```

## Scope — product work, not research

The pipeline runs in production. What's left is product work, not research:

- [x] `FlightClient` API surface — `connect()` / `query()` (async-iterate OR await it) /
  `from()` builder / `capabilities()` / `tables()` / `schema()` / raw `getFlightInfo()`+`doGet()` —
  see [docs/api-m1.md](docs/api-m1.md)
- [x] multi-batch and dictionary IPC (multi-endpoint FlightInfo, per-endpoint readers)
- [x] test coverage — unit + live integration against three dialects
  (Sparrow · GizmoSQL · ROAPI) + an automated headless-Chrome smoke (`npm run test:browser`)
- [ ] npm packaging and publishing (M4 — the API above ships as written)

## The Sparrow family

One transport, many clients: [Sparrow](https://sparrowflight.io) (the Flight server) ·
[sparrowXL](https://sparrowflight.io/excel) (Excel) ·
[sparrowCLI](https://sparrowflight.io/cli) (terminal) ·
[sparrowMCP](https://sparrowflight.io/mcp) (AI agents) ·
**sparrowJS** (the browser).

## License

[Apache-2.0](LICENSE)
