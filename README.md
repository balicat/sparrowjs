# @sparrowjs/flight

**The missing browser client for Apache Arrow Flight** — a browser implementation of
Apache Arrow Flight and Flight SQL over gRPC-web. Works with any Apache Arrow Flight
or Flight SQL server.

> **Status** &nbsp; ✔ powers the [live demo at sparrowflight.io](https://sparrowflight.io/demo/js) &nbsp;·&nbsp; ⚠ API not yet stable, not yet on npm
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

## The API, as intended

```js
import { FlightClient } from "@sparrowjs/flight";

const client = await FlightClient.connect({
  endpoint: "/flight",
  user: "demo",   // Basic in, Bearer adopted automatically
});

// Flight SQL — the everyday path
const stream = client.query(`
  SELECT period, value FROM series_data
  WHERE series_id = 'PET.RWTC.D'
`);
for await (const batch of stream) {
  chart.append(batch); // Apache Arrow JS RecordBatch
}

// raw Flight against any server: client.getFlightInfo(desc) → client.doGet(ticket)
```

What runs today is the demo factory (`src/demo-entry.js`) — a `createSparrowClient()`
that speaks Flight SQL (`CommandStatementQuery` → `GetFlightInfo` → `DoGet`), streams
record batches as they arrive (`onBatch` callback), and returns the assembled Arrow
table plus wire timings. The `FlightClient` API above is the packaging target.

## How it works

```
your browser
    │  sparrowJS (@sparrowjs/flight)
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
| 1 series · 10,217 rows | 345 ms · 201 KB | **240 ms** · 56 KB gz | REST — one round trip beats Flight's two |
| 10 series · 71,979 rows | **588 ms** · 1.7 MB | 850 ms · 4.8 MB JSON | Arrow — the backend's JSON factory becomes the bottleneck |

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
| **`@sparrowjs/flight`** | **browser-first** (proven in Node too) | **gRPC-web** | ✅ |

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

- [ ] `FlightClient` API polish (connect / doGet / query surface above)
- [ ] multi-batch and dictionary IPC edge cases
- [ ] test coverage
- [ ] npm packaging and publishing

## The Sparrow family

One transport, many clients: [Sparrow](https://sparrowflight.io) (the Flight server) ·
[sparrowXL](https://sparrowflight.io/excel) (Excel) ·
[sparrowCLI](https://sparrowflight.io/cli) (terminal) ·
[sparrowMCP](https://sparrowflight.io/mcp) (AI agents) ·
**sparrowJS** (the browser).

## License

[Apache-2.0](LICENSE)
