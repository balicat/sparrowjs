# sparrowjs — M1 API surface (draft for review)

*2026-07-15. React to this before any code. The constraint set: (1) the README
already promises `for await (const batch of client.query(sql))` — that shape is
public and must work verbatim; (2) the demo factory's `{ table, timing }` +
`onBatch` is proven live and its users (the demo page) shouldn't need a rewrite;
(3) the sparrowCLI lessons (capabilities decode, bigint, stats anatomy, dialect
quirks) get designed in now, not bolted on.*

## Design principles

- **One method, two consumption styles.** `query()` returns a `QueryStream`
  that is *both* async-iterable (batches as they arrive) *and* awaitable
  (assembled table + stats). No `queryStream()` vs `queryTable()` split.
- **Read-only, browser-first.** No DoPut, no Handshake. Node works over the
  same transport (that's the test story), the browser is the headline.
- **`apache-arrow` is a peer dependency.** We hand back its RecordBatch/Table
  types untranslated. Zero-copy typed arrays are the value; we never re-wrap.
- **Metadata is free.** The auth bootstrap *is* a GetSqlInfo call — so
  `capabilities()` costs nothing extra. Decode it once, cache it.

## The surface

```ts
import { connect, FlightClient } from "sparrowjs";

// ── connect ──────────────────────────────────────────────────────────────
const client: FlightClient = await connect({
  endpoint: "/flight",            // gRPC-web edge (Envoy grpc_web / Traefik)
  user: "demo", pass: "demo",     // Basic in → Bearer adopted automatically
  // auth: { bearer: token },     // or bring your own token
  // headers: { database: "x" },  // per-call extras (InfluxDB 3)
  // bigIntMode: "number",        // see §bigint — default "bigint" (lossless)
});
// connect() performs the auth bootstrap (GetSqlInfo, session-less) EAGERLY:
// bad endpoint or creds fail here, not on the first query. Single-flighted —
// concurrent callers share one bootstrap (the demo's race fix, kept).
// The same response seeds capabilities(). One RTT, three jobs.

// ── query: the everyday path ─────────────────────────────────────────────
// Style A — streaming (the README promise):
for await (const batch of client.query(sql)) {
  chart.append(batch);            // apache-arrow RecordBatch, as it arrives
}

// Style B — one-shot (the demo factory shape, formalized):
const { table, stats } = await client.query(sql);
//    table: apache-arrow Table
//    stats: see §stats

// Style C — both at once (stream to a chart AND keep the table):
const { table, stats } = await client.query(sql, {
  onBatch(batch, i, msSinceStart) { chart.append(batch); },
});

// ── QueryStream ──────────────────────────────────────────────────────────
interface QueryStream extends AsyncIterable<RecordBatch>, PromiseLike<QueryResult> {
  cancel(): void;                 // abort the fetch AND stop batch delivery —
                                  // in-flight consumption rejects, even when the
                                  // transport already buffered the whole response
  readonly schema: Promise<Schema>; // resolves at first frame; ACCESSING it starts
                                  // the query lazily and decodes exactly one frame
                                  // (buffered + replayed to a later iterator), so a
                                  // standalone `await stream.schema` settles alone
}
interface QueryResult {
  table: Table;
  stats: QueryStats;
}
// Iterating consumes the stream; awaiting consumes-and-assembles. Doing both
// on one QueryStream is fine (await after for-await returns the same result);
// starting to iterate twice throws.

// ── stats (CLI --stats, minus the terminal) ──────────────────────────────
interface QueryStats {
  authMs: number;                 // bootstrap share (0 on warm client)
  planMs: number;                 // GetFlightInfo round trip
  firstBatchMs: number;           // plan→first decoded batch
  streamMs: number;               // first→last batch
  totalMs: number;
  rows: number;
  batches: number;
  wireBytes: number;              // FlightData frames as received
  rowsPerSec: number;
  mbitPerSec: number;
}
// Every dashboard gets its "83,490 rows in 86 ms" line for free.

// ── typed query builder (the thing a CLI can't be) ───────────────────────
const stream = client
  .from("series_data")
  .select("period", "value")      // omit → *
  .where("series_id = 'PET.RWTC.D'")
  .orderBy("period", "desc")
  .limit(1000)
  .query();                       // → QueryStream, same dual nature
// Builds the SELECT exactly like `sparrow query` does. .toSQL() exposes the
// string. Deliberately shallow — a WHERE-string, not an expression DSL.
// ⚠ where()/select()/orderBy() are VERBATIM, UNSANITIZED passthrough into
// SQL text. Never interpolate untrusted (user) input — sanitize or whitelist
// in the caller. This is a query BUILDER, not an escaping layer.
// (Natural place to emit Substrait later; out of scope for M1.)

// ── metadata (Tier 2, the CLI's hard-won logic) ──────────────────────────
const caps = client.capabilities();       // sync — cached from bootstrap
// { vendorName, vendorVersion, arrowVersion, readOnly, sql,
//   substrait, transactions, cancel, raw: Map<number, unknown> }
// fields a server didn't advertise are undefined — never a fabricated false

const tabs = await client.tables();       // GetTables RPC (portable path)
// [{ catalog, dbSchema, name, type }]    // includes MACRO rows (search_meta)

const schema = await client.schema("series_data");  // arrow Schema
// GetTables include_schema=true, fallback GetSchema(SELECT * LIMIT 0)

// ── raw Flight escape hatches (any Flight server, not just SQL) ──────────
const info = await client.getFlightInfo(descriptor);
for await (const batch of client.doGet(ticket)) { ... }
// doGet() reuses the same IPC-reassembly + decode pipeline — JSON-ticket
// servers and non-SQL Flight servers get streaming decode for free.
```

## §bigint

More acute in JS than anywhere: int64 → `BigInt64Array` is what Arrow JS
gives us (lossless), but `JSON.stringify` throws on BigInt and most chart
libs choke. `bigIntMode` (client-level, overridable per query):

| mode | behavior | when |
|---|---|---|
| `"bigint"` *(default)* | untouched BigInt64Array — zero-copy, lossless | you control the consumer |
| `"number"` | converted via `Number()`, **throws** if any value exceeds 2^53 | charts; fail loud beats silent precision loss |
| `"string"` | converted to strings | JSON round-trips (CLI `--bigint-as-string` twin) |

Conversion materializes the column (documented cost); `"bigint"` stays
zero-copy. Applies to the convenience paths (`table`, `onBatch` batches);
raw `doGet` is always untouched.

## View types (Utf8View/BinaryView) — transcoded client-side

Arrow JS has no View-type decode (upstream gap), and DataFusion-family
servers emit Utf8View for every parquet-sourced string column. sparrowJS
**transcodes flat Utf8View/BinaryView columns to classic Utf8/Binary at the
IPC layer, before Arrow JS sees them** (`view-transcode.ts`) — the same
transformation `schema_force_view_types=false` performs server-side, done in
the client so modern DataFusion servers work as-is, no server config, no
asking the operator. Zero-cost passthrough when the schema has no View
columns; strings are copied once (inherent — the classic layout requires it).

Not covered (rare): View types nested inside List/Struct, dictionary-encoded
View columns, compressed bodies carrying View columns, >2 GiB per column per
batch. Those throw a decorated error naming the type and the server-side fix
(the raw upstream message is kept as `cause`).

## Explicitly NOT in M1

- **M2**: error taxonomy (typed `FlightError` with layer + grpc code —
  today errors pass through from connect-es), backpressure, bundle budget
  (< 30 kB min+gz excl. apache-arrow), auth adapter plugins.
- **Tier 3**: `search()` (server FTS), `profile()`.
- **Never** (CLI features with no browser analog): audit, doctor, check,
  diff, ping, feedback, completion, encrypted parquet.

## Decision points

1. **`connect()` bootstraps eagerly** (fail fast, capabilities cached, one
   RTT). Alternative: lazy on first query — saves the RTT when a page
   connects speculatively. *Recommend eager; it's what the README's `await
   connect()` implies.*
2. **`stats` is always on** (the measurement is free — timestamps around
   work we already do). Alternative: opt-in flag. *Recommend always-on.*
3. **Builder entry is `client.from(table)`** — reads like SQL. Alternative:
   `client.table(name)` or a standalone `from()`. *Recommend `client.from`.*
4. **`bigIntMode` default `"bigint"`** — lossless-by-default, consumers
   opt into convenience. Alternative: `"number"` default matches what JS
   devs expect but silently caps at 2^53 unless we throw (we would throw).
   *Recommend `"bigint"` default + loud `"number"`.*
5. **Package layout**: single entry `sparrowjs` (~everything above,
   tree-shakeable ESM) vs a `/raw` subpath for the escape hatches.
   *Recommend single entry for M1 — subpaths when the bundle budget bites.*
