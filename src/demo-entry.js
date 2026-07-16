// sparrowJS demo bundle — a thin adapter over the library.
// The demo page (sparrowflight.io/demo/js) predates the library and expects
// the M0 factory shape ({ table, rows, cols, bytes, batches, timing }); this
// keeps that surface while ALL the real work happens in @sparrowjs/flight —
// so the live demo now exercises the shipped code: single-flight bootstrap,
// streaming decode, the View-type transcoder, the FlightInfo schema fallback.
//
// Build (from repo root — the library must be built first):
//   npm run build && npm run demo:bundle
import { FlightClient } from "../dist/index.js";

export function createSparrowClient({ endpoint, user, pass }) {
  const client = new FlightClient({ endpoint, user, pass });
  // the demo wants a synchronous factory: skip eager connect() and let the
  // first query trigger the (single-flighted) bootstrap, as M0 did
  return {
    async query(sql, opts = {}) {
      const { table, stats } = await client.query(sql, { onBatch: opts.onBatch });
      return {
        table,
        rows: table.numRows,
        cols: table.schema.fields.map((f) => f.name),
        bytes: stats.wireBytes,
        batches: stats.batches,
        timing: {
          auth: Math.round(stats.authMs),
          plan: Math.round(stats.planMs),
          firstBatch: Math.round(stats.firstBatchMs),
          total: Math.round(stats.totalMs),
        },
      };
    },
  };
}
