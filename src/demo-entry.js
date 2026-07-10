// sparrowJS demo bundle — the code on sparrowflight.io/demo is this file.
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

function pad8(n) {
  return (n + 7) & ~7;
}
function encapsulate(dataHeader, dataBody) {
  const metaLen = pad8(dataHeader.length);
  const buf = new Uint8Array(8 + metaLen + pad8(dataBody.length));
  const dv = new DataView(buf.buffer);
  dv.setUint32(0, 0xffffffff, true);
  dv.setUint32(4, metaLen, true);
  buf.set(dataHeader, 8);
  buf.set(dataBody, 8 + metaLen);
  return buf;
}
const EOS = new Uint8Array([0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0]);

export function createSparrowClient({ endpoint, user, pass }) {
  const transport = createGrpcWebTransport({ baseUrl: endpoint });
  const client = createClient(FlightService, transport);
  let auth = "Basic " + btoa(`${user}:${pass ?? ""}`);
  let bootstrapped = false;

  const callOpts = () => ({
    headers: { authorization: auth },
    onHeader(h) {
      const a = h.get("authorization");
      if (!a) return;
      const m = a.match(/Bearer\s+([^\s,]+)/i);
      auth = m ? `Bearer ${m[1]}` : a;
    },
  });

  function descFor(schema, msg) {
    return create(FlightDescriptorSchema, {
      type: FlightDescriptor_DescriptorType.CMD,
      cmd: toBinary(AnySchema, anyPack(schema, msg)),
    });
  }

  async function bootstrap() {
    if (bootstrapped) return;
    await client.getFlightInfo(
      descFor(CommandGetSqlInfoSchema, create(CommandGetSqlInfoSchema, { info: [] })),
      callOpts(),
    );
    bootstrapped = true;
  }

  async function query(sql) {
    const t0 = performance.now();
    await bootstrap();
    const tAuth = performance.now();

    const info = await client.getFlightInfo(
      descFor(CommandStatementQuerySchema, create(CommandStatementQuerySchema, { query: sql })),
      callOpts(),
    );
    const tInfo = performance.now();

    const chunks = [];
    let frames = 0;
    let tFirst = 0;
    for await (const fd of client.doGet(info.endpoint[0].ticket, callOpts())) {
      if (++frames === 1) tFirst = performance.now();
      chunks.push(encapsulate(fd.dataHeader, fd.dataBody));
    }
    chunks.push(EOS);
    const total = chunks.reduce((n, c) => n + c.length, 0);
    const ipc = new Uint8Array(total);
    let off = 0;
    for (const c of chunks) {
      ipc.set(c, off);
      off += c.length;
    }
    const table = tableFromIPC(ipc);
    const tEnd = performance.now();

    return {
      table,
      rows: table.numRows,
      cols: table.schema.fields.map((f) => f.name),
      bytes: total,
      timing: {
        auth: Math.round(tAuth - t0),
        plan: Math.round(tInfo - tAuth),
        firstBatch: Math.round(tFirst - tInfo),
        total: Math.round(tEnd - t0),
      },
    };
  }

  return { query };
}
