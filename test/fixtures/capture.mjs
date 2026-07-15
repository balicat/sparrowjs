// Capture a raw encapsulated-IPC fixture from a live gRPC-web endpoint —
// used to freeze real Utf8View bytes (Arrow JS cannot produce them itself).
//   node test/fixtures/capture.mjs "SELECT ..." out.bin [endpoint] [user] [pass]
import { writeFileSync } from "node:fs";
import { createClient } from "@connectrpc/connect";
import { createGrpcWebTransport } from "@connectrpc/connect-web";
import { create, toBinary } from "@bufbuild/protobuf";
import { AnySchema, anyPack } from "@bufbuild/protobuf/wkt";
import {
  FlightService,
  FlightDescriptorSchema,
  FlightDescriptor_DescriptorType,
} from "../../src/gen/Flight_pb.js";
import { CommandStatementQuerySchema } from "../../src/gen/FlightSql_pb.js";
import { encapsulate, EOS } from "../../dist/lib/ipc.js";

const [, , QUERY, OUT, ENDPOINT = "https://sparrowflight.io/flight-roapi", USER = "demo", PASS = "demo"] =
  process.argv;
if (!QUERY || !OUT) {
  console.error('usage: node capture.mjs "SELECT ..." out.bin [endpoint] [user] [pass]');
  process.exit(3);
}

let auth = "Basic " + Buffer.from(`${USER}:${PASS}`).toString("base64");
const callOpts = () => ({
  headers: { authorization: auth },
  onHeader(h) {
    const a = h.get("authorization");
    const m = a?.match(/Bearer\s+([^\s,]+)/i);
    if (m) auth = `Bearer ${m[1]}`;
  },
});

const client = createClient(FlightService, createGrpcWebTransport({ baseUrl: ENDPOINT }));
const desc = create(FlightDescriptorSchema, {
  type: FlightDescriptor_DescriptorType.CMD,
  cmd: toBinary(AnySchema, anyPack(CommandStatementQuerySchema, create(CommandStatementQuerySchema, { query: QUERY }))),
});
const info = await client.getFlightInfo(desc, callOpts());
const chunks = [];
for await (const fd of client.doGet(info.endpoint[0].ticket, callOpts())) {
  chunks.push(encapsulate(fd.dataHeader, fd.dataBody));
}
chunks.push(EOS);
const total = chunks.reduce((n, c) => n + c.length, 0);
const out = new Uint8Array(total);
let off = 0;
for (const c of chunks) {
  out.set(c, off);
  off += c.length;
}
writeFileSync(OUT, out);
console.log(`${chunks.length - 1} messages · ${total} bytes → ${OUT}`);
