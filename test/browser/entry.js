// Browser smoke — bundled by run.mjs, executed in headless Chrome against a
// live gRPC-web edge. Results land on window.__RESULT for the runner to read.
import { connect } from "../../dist/index.js";

const out = { ok: false, steps: [], error: null };
window.__RESULT = null;

(async () => {
  try {
    const client = await connect({ endpoint: window.__ENDPOINT, user: "demo", pass: "" });
    const caps = client.capabilities();
    out.steps.push(["capabilities", caps.vendorName ?? "(none)", `substrait=${caps.substrait}`]);

    // streaming: first batch must land before the stream ends (the M3 pitch)
    let firstMs = 0;
    const { table, stats } = await client.query(
      "SELECT series_id, period, value FROM series_data LIMIT 10000",
      { onBatch: (_b, _i, ms) => { if (!firstMs) firstMs = ms; } },
    );
    out.steps.push(["stream", `${table.numRows} rows`, `${stats.batches} batches`, `first ${firstMs} ms`, `total ${stats.totalMs} ms`]);
    if (table.numRows !== 10000) throw new Error(`rows ${table.numRows}`);
    if (stats.batches < 2) throw new Error(`expected multi-batch, got ${stats.batches}`);
    if (!(firstMs < stats.totalMs)) throw new Error("first batch did not precede stream end");

    const r2 = await client.from("series_data").select("series_id").limit(3).query();
    if (r2.table.numRows !== 3) throw new Error("builder rows");
    out.steps.push(["builder", `plan ${r2.stats.planMs} ms`]);

    const tabs = await client.tables();
    if (!tabs.some((t) => t.name === "series_data")) throw new Error("tables missing series_data");
    out.steps.push(["tables", tabs.length]);

    const r3 = await client.query("SELECT 9007199254740993 AS x", { bigIntMode: "string" });
    if (r3.table.getChild("x").get(0) !== "9007199254740993") throw new Error("bigint string");
    out.steps.push(["bigint-string", "ok"]);

    out.ok = true;
  } catch (e) {
    out.error = String((e && e.stack) || e);
  }
  window.__RESULT = out;
})();
