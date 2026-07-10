// sparrowJS M0 — Arrow Flight over gRPC-web, decoded by Apache Arrow JS.
// Node runs the same transport a browser would (fetch + ReadableStream).
import { createClient } from "@connectrpc/connect";
import { createGrpcWebTransport } from "@connectrpc/connect-web";
import { create, toBinary } from "@bufbuild/protobuf";
import { AnySchema, anyPack } from "@bufbuild/protobuf/wkt";
import { tableFromIPC } from "apache-arrow";

import {
  FlightService,
  FlightDescriptorSchema,
  FlightDescriptor_DescriptorType,
} from "./gen/Flight_pb.js";
import {
  CommandStatementQuerySchema,
  CommandGetSqlInfoSchema,
} from "./gen/FlightSql_pb.js";

const ENDPOINT = process.env.SPARROW_ENDPOINT;
const USER = process.env.SPARROW_USER ?? "";
const PASS = process.env.SPARROW_PASS ?? "";
if (!ENDPOINT) {
  console.error("usage: SPARROW_ENDPOINT=http://host:8890 [SPARROW_USER=u SPARROW_PASS=p] node src/m0.mjs \"SELECT 42\"");
  console.error("       (endpoint = a gRPC-web edge in front of your Flight SQL server)");
  process.exit(1);
}
const QUERY =
  process.argv[2] ?? "SELECT range AS n, range * 1.5 AS v FROM range(5)";

// GizmoSQL (like EnergyScope) mints a Bearer from Basic and binds the session
// to the Bearer — so adopt the token the server hands back (same trick the
// ADBC driver does under the hood).
let auth = "Basic " + Buffer.from(`${USER}:${PASS}`).toString("base64");
const callOpts = () => ({
  headers: { authorization: auth },
  onHeader(h) {
    const a = h.get("authorization");
    if (!a) return;
    // Headers.get comma-joins multiple values — take the Bearer token only
    const m = a.match(/Bearer\s+([^\s,]+)/i);
    auth = m ? `Bearer ${m[1]}` : a;
  },
});

// ── Flight IPC reassembly: FlightData frames -> one Arrow IPC stream ─────
function pad8(n) {
  return (n + 7) & ~7;
}
function encapsulate(dataHeader, dataBody) {
  const metaLen = pad8(dataHeader.length);
  const buf = new Uint8Array(8 + metaLen + pad8(dataBody.length));
  const dv = new DataView(buf.buffer);
  dv.setUint32(0, 0xffffffff, true); // continuation marker
  dv.setUint32(4, metaLen, true); // metadata length (incl. padding)
  buf.set(dataHeader, 8);
  buf.set(dataBody, 8 + metaLen);
  return buf;
}
const EOS = new Uint8Array([0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0]);

// ── the M0 flow ──────────────────────────────────────────────────────────
const transport = createGrpcWebTransport({ baseUrl: ENDPOINT });
const client = createClient(FlightService, transport);

console.log(`sparrowJS M0 → ${ENDPOINT} (gRPC-web)`);
console.log(`query: ${QUERY}`);

const t0 = performance.now();

function descFor(schema, msg) {
  return create(FlightDescriptorSchema, {
    type: FlightDescriptor_DescriptorType.CMD,
    cmd: toBinary(AnySchema, anyPack(schema, msg)),
  });
}

// auth bootstrap: GetSqlInfo runs session-less under Basic; the response
// carries the Bearer we adopt for everything after
await client.getFlightInfo(
  descFor(CommandGetSqlInfoSchema, create(CommandGetSqlInfoSchema, { info: [] })),
  callOpts(),
);
console.log(
  `✓ auth bootstrap: ${auth.startsWith("Bearer") ? "Bearer adopted" : "still Basic"} in ${(performance.now() - t0).toFixed(0)} ms`,
);

const info = await client.getFlightInfo(
  descFor(CommandStatementQuerySchema, create(CommandStatementQuerySchema, { query: QUERY })),
  callOpts(),
);
console.log(
  `✓ GetFlightInfo: ${info.endpoint.length} endpoint(s) in ${(performance.now() - t0).toFixed(0)} ms`,
);

const chunks = [];
let frames = 0;
for await (const fd of client.doGet(info.endpoint[0].ticket, callOpts())) {
  frames++;
  chunks.push(encapsulate(fd.dataHeader, fd.dataBody));
}
chunks.push(EOS);
console.log(`✓ DoGet: ${frames} FlightData frames streamed`);

const total = chunks.reduce((n, c) => n + c.length, 0);
const ipcBytes = new Uint8Array(total);
let off = 0;
for (const c of chunks) {
  ipcBytes.set(c, off);
  off += c.length;
}

const table = tableFromIPC(ipcBytes);
console.log(
  `✓ Arrow JS decoded: ${table.numRows} rows × ${table.numCols} cols in ${(performance.now() - t0).toFixed(0)} ms total`,
);
console.log(`  schema: ${table.schema.fields.map((f) => `${f.name}:${f.type}`).join(", ")}`);
console.log(`  rows:`, table.toArray().map((r) => r.toJSON()));
