// sparrowJS demo bundle — the code on sparrowflight.io/demo is this file.
import { createClient } from "@connectrpc/connect";
import { createGrpcWebTransport } from "@connectrpc/connect-web";
import { create, toBinary } from "@bufbuild/protobuf";
import { AnySchema, anyPack } from "@bufbuild/protobuf/wkt";
import { RecordBatchReader, Table } from "apache-arrow";

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
  let bootstrapping = null;

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

  // Single-flight: concurrent first queries must share one bootstrap, or the
  // second Basic call races the first's Bearer adoption mid-flight.
  function bootstrap() {
    bootstrapping ??= client
      .getFlightInfo(
        descFor(CommandGetSqlInfoSchema, create(CommandGetSqlInfoSchema, { info: [] })),
        callOpts(),
      )
      .then(() => undefined);
    return bootstrapping;
  }

  // query(sql, { onBatch }) — batches decode AS THEY ARRIVE off the wire.
  // onBatch(batch, batchIndex, msSinceStart) fires per Arrow RecordBatch, so a
  // chart can render before the stream ends. Returns the assembled Table plus
  // wire timings, same shape as before.
  async function query(sql, opts = {}) {
    const t0 = performance.now();
    await bootstrap();
    const tAuth = performance.now();

    const info = await client.getFlightInfo(
      descFor(CommandStatementQuerySchema, create(CommandStatementQuerySchema, { query: sql })),
      callOpts(),
    );
    const tInfo = performance.now();

    let bytes = 0;
    async function* ipcStream() {
      for await (const fd of client.doGet(info.endpoint[0].ticket, callOpts())) {
        const chunk = encapsulate(fd.dataHeader, fd.dataBody);
        bytes += chunk.length;
        yield chunk;
      }
      yield EOS;
    }

    const reader = await RecordBatchReader.from(ipcStream());
    const batches = [];
    let tFirst = 0;
    for await (const batch of reader) {
      batches.push(batch);
      if (!tFirst) tFirst = performance.now();
      if (opts.onBatch) {
        try {
          opts.onBatch(batch, batches.length, Math.round(performance.now() - t0));
        } catch (_) { /* a rendering hiccup must not kill the stream */ }
      }
    }
    const table = new Table(reader.schema, batches);
    const tEnd = performance.now();

    return {
      table,
      rows: table.numRows,
      cols: table.schema.fields.map((f) => f.name),
      bytes,
      batches: batches.length,
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
