// Live integration: the browser transport (gRPC-web) from Node against the
// public demo endpoints. Override with SPARROW_ENDPOINT/SPARROW_USER/SPARROW_PASS.
import { test } from "node:test";
import assert from "node:assert/strict";
import { connect } from "../dist/index.js";

const ORIGIN = process.env.SPARROW_ORIGIN ?? "https://sparrowflight.io";
const SPARROW = {
  endpoint: process.env.SPARROW_ENDPOINT ?? `${ORIGIN}/flight`,
  user: process.env.SPARROW_USER ?? "demo",
  pass: process.env.SPARROW_PASS ?? "",
};

test("connect() bootstraps eagerly and seeds capabilities()", async () => {
  const client = await connect(SPARROW);
  const caps = client.capabilities();
  assert.ok(caps.raw.size > 0, "GetSqlInfo decoded");
  assert.equal(caps.sql, true);
  assert.equal(typeof caps.vendorName, "string");
  console.log(
    `    capabilities: ${caps.vendorName} ${caps.vendorVersion ?? ""} · substrait=${caps.substrait} · readOnly=${caps.readOnly}`,
  );
});

test("style A — for await batches (the README promise)", async () => {
  const client = await connect(SPARROW);
  let rows = 0;
  let batches = 0;
  for await (const batch of client.query("SELECT range AS n, range * 1.5 AS v FROM range(100)")) {
    rows += batch.numRows;
    batches++;
  }
  assert.equal(rows, 100);
  assert.ok(batches >= 1);
});

test("style B — await → { table, stats }", async () => {
  const client = await connect(SPARROW);
  const { table, stats } = await client.query("SELECT range AS n FROM range(50)");
  assert.equal(table.numRows, 50);
  assert.equal(stats.rows, 50);
  assert.ok(stats.planMs > 0, "planMs measured");
  assert.ok(stats.wireBytes > 0, "wireBytes counted");
  assert.ok(stats.totalMs >= stats.streamMs);
  console.log(
    `    stats: plan ${stats.planMs} ms · first batch ${stats.firstBatchMs} ms · total ${stats.totalMs} ms · ${stats.wireBytes} B`,
  );
});

test("style C — onBatch fires per batch AND the table assembles", async () => {
  const client = await connect(SPARROW);
  const seen = [];
  const { table } = await client.query("SELECT range AS n FROM range(10)", {
    onBatch: (b, i, ms) => seen.push([i, b.numRows, ms >= 0]),
  });
  assert.equal(table.numRows, 10);
  assert.ok(seen.length >= 1);
  assert.equal(seen[0][0], 1, "1-based batch index");
});

test("await after for-await returns the same accumulated result", async () => {
  const client = await connect(SPARROW);
  const stream = client.query("SELECT range AS n FROM range(25)");
  let iterated = 0;
  for await (const b of stream) iterated += b.numRows;
  const { table } = await stream;
  assert.equal(iterated, 25);
  assert.equal(table.numRows, 25);
});

test("iterating twice throws", async () => {
  const client = await connect(SPARROW);
  const stream = client.query("SELECT 1");
  for await (const _ of stream) {
    void _;
  }
  await assert.rejects(async () => {
    for await (const _ of stream) void _;
  }, /already consumed/);
});

test("stream.schema resolves during iteration, before the stream ends", async () => {
  const client = await connect(SPARROW);
  const stream = client.query("SELECT range AS n FROM range(5)");
  const it = stream[Symbol.asyncIterator]();
  await it.next(); // first batch in
  const schema = await stream.schema;
  assert.deepEqual(
    schema.fields.map((f) => f.name),
    ["n"],
  );
  await it.return?.();
});

test("empty result still carries a schema (LIMIT 0)", async () => {
  const client = await connect(SPARROW);
  const { table } = await client.query("SELECT range AS n FROM range(10) LIMIT 0");
  assert.equal(table.numRows, 0);
  assert.deepEqual(
    table.schema.fields.map((f) => f.name),
    ["n"],
  );
});

test("builder — from().select().where().limit() against series_data", async () => {
  const client = await connect(SPARROW);
  const { table, stats } = await client
    .from("series_data")
    .select("series_id", "period", "value")
    .limit(10)
    .query();
  assert.equal(table.numRows, 10);
  assert.ok(stats.rows === 10);
});

test("tables() — GetTables discovery, series_data + the search_meta MACRO", async () => {
  const client = await connect(SPARROW);
  const tabs = await client.tables();
  assert.ok(tabs.some((t) => t.name === "series_data"), "series_data listed");
  const macro = tabs.find((t) => t.type === "MACRO");
  console.log(
    `    tables: ${tabs.length} rows${macro ? ` · MACRO advertised: ${macro.name}` : ""}`,
  );
});

test("schema(series_data)", async () => {
  const client = await connect(SPARROW);
  const schema = await client.schema("series_data");
  const names = schema.fields.map((f) => f.name);
  assert.ok(names.includes("series_id"), `series_id in ${names.join(",")}`);
});

test("bigIntMode — bigint (default) is lossless BigInt", async () => {
  const client = await connect(SPARROW);
  const { table } = await client.query("SELECT 9007199254740993 AS big");
  assert.equal(table.getChild("big").get(0), 9007199254740993n);
});

test('bigIntMode "number" throws loud past 2^53', async () => {
  const client = await connect(SPARROW);
  await assert.rejects(
    client.query("SELECT 9007199254740993 AS big", { bigIntMode: "number" }),
    RangeError,
  );
});

test('bigIntMode "number" converts safe values; "string" keeps precision', async () => {
  const client = await connect(SPARROW);
  const { table: n } = await client.query("SELECT 42::BIGINT AS x", { bigIntMode: "number" });
  assert.equal(n.getChild("x").get(0), 42);
  const { table: s } = await client.query("SELECT 9007199254740993 AS x", { bigIntMode: "string" });
  assert.equal(s.getChild("x").get(0), "9007199254740993");
});

test("multi-batch production pull decodes (dictionaries included if sent)", async () => {
  const client = await connect(SPARROW);
  const { table, stats } = await client.query(
    "SELECT series_id, period, value FROM series_data LIMIT 10000",
  );
  assert.equal(table.numRows, 10000);
  console.log(
    `    prod pull: ${stats.rows} rows · ${stats.batches} batches · ${stats.wireBytes} B · ${stats.totalMs} ms · schema ${table.schema.fields.map((f) => `${f.name}:${f.type}`).join(", ")}`,
  );
});

test("early break then await → partial table, no hang", async () => {
  const client = await connect(SPARROW);
  const stream = client.query("SELECT series_id, period, value FROM series_data LIMIT 10000");
  for await (const _ of stream) {
    void _;
    break; // abandon after the first batch
  }
  const { table } = await stream;
  assert.ok(table.numRows >= 1, "partial rows kept");
  assert.ok(table.numRows <= 10000);
});

test("query error surfaces (bad SQL rejects)", async () => {
  const client = await connect(SPARROW);
  await assert.rejects(client.query("SELECT FROM nothing sensible"));
});

test("connect() fails fast on bad credentials", async () => {
  // the Sparrow python node is keyless for reads; GizmoSQL enforces creds
  await assert.rejects(
    connect({ endpoint: `${ORIGIN}/flight-gizmo`, user: "demo", pass: "WRONG" }),
  );
});

test("dialect: GizmoSQL (DuckDB C++) over the same wire", async () => {
  const client = await connect({ endpoint: `${ORIGIN}/flight-gizmo`, user: "demo", pass: "demo" });
  const { table } = await client.query("SELECT 42 AS answer");
  assert.equal(Number(table.getChild("answer").get(0)), 42);
});

// ── regressions from the tester's M1 adversarial pass (2026-07-15) ────────

test("F1: standalone `await stream.schema` settles without consuming", async () => {
  const client = await connect(SPARROW);
  const stream = client.query("SELECT range AS n FROM range(100)");
  const schema = await Promise.race([
    stream.schema,
    new Promise((_, rej) => setTimeout(() => rej(new Error("schema deadlock (F1)")), 8000)),
  ]);
  assert.deepEqual(schema.fields.map((f) => f.name), ["n"]);
  // and the stream is still fully consumable afterward — primed frame replays
  let rows = 0;
  for await (const b of stream) rows += b.numRows;
  assert.equal(rows, 100);
});

test("F2: cancel() from onBatch rejects even on a small, fully-buffered result", async () => {
  const client = await connect(SPARROW);
  const stream = client.query("SELECT range AS n FROM range(300000)", {
    onBatch: () => stream.cancel(),
  });
  await assert.rejects(stream, /cancel/i);
});

test("F2: cancel() before any consumption rejects the await", async () => {
  const client = await connect(SPARROW);
  const stream = client.query("SELECT range AS n FROM range(1000)");
  stream.cancel();
  await assert.rejects(stream);
});

test("F5→transcode: ROAPI Utf8View string columns now decode client-side", async () => {
  const client = await connect({ endpoint: `${ORIGIN}/flight-roapi`, user: "demo", pass: "demo" });
  const { table } = await client.query("SELECT series_id FROM series_data LIMIT 5");
  assert.equal(table.numRows, 5);
  assert.equal(String(table.schema.fields[0].type), "Utf8");
  const v = table.getChild("series_id").get(0);
  assert.ok(typeof v === "string" && v.length > 0, `got ${v}`);
});

test("transcode cross-vendor: roapi (Utf8View) values == duckdb (classic) values", async () => {
  const sql = "SELECT series_id FROM series_data ORDER BY series_id LIMIT 5";
  const roapi = await connect({ endpoint: `${ORIGIN}/flight-roapi`, user: "demo", pass: "demo" });
  const duck = await connect(SPARROW);
  const [a, b] = await Promise.all([roapi.query(sql), duck.query(sql)]);
  assert.deepEqual(
    a.table.toArray().map((r) => r.series_id),
    b.table.toArray().map((r) => r.series_id),
  );
});

test("dialect: ROAPI (DataFusion) — connect tolerates missing GetSqlInfo", async () => {
  const client = await connect({ endpoint: `${ORIGIN}/flight-roapi`, user: "demo", pass: "demo" });
  const caps = client.capabilities(); // may be empty — that's the point
  const { table } = await client.query("SELECT 1 AS one");
  assert.equal(Number(table.getChild("one").get(0)), 1);
  console.log(`    roapi: caps.raw.size=${caps.raw.size} (empty tolerated)`);
});
