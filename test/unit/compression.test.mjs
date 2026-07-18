// Compression decode: registering lz4js on apache-arrow's compressionRegistry
// (via sparrowjs) lets arrow-js decode LZ4-frame-compressed IPC streams — the
// format Sparrow/DuckDB/GizmoSQL emit. Fixtures are real: the SAME 8,000-row
// query written both uncompressed (lz4-plain) and lz4-compressed (lz4-compressed),
// so a decode bug shows as a value mismatch.
//
// Feature-gated: apache-arrow < 21.2 has no compressionRegistry (the hook
// landed in PR #14 → 21.2). On those versions the whole suite is skipped, so
// the repo stays green whether CI pins 21.1 or 21.2.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import * as arrow from "apache-arrow";
import { registerCompressionCodecs } from "../../dist/lib/compression.js";

const { tableFromIPC } = arrow;
// namespace access, NOT a named import: apache-arrow < 21.2 doesn't export
// compressionRegistry, and a named import of a missing export throws at
// module load (breaking CI on the currently-released Arrow).
const HAS_REGISTRY = typeof arrow.compressionRegistry?.set === "function";
const fx = (n) => new Uint8Array(readFileSync(new URL(`../fixtures/${n}`, import.meta.url)));

test("sparrowjs registers an LZ4_FRAME decompressor (no-op < 21.2)", () => {
  registerCompressionCodecs(); // idempotent; also runs in the FlightClient ctor
  // nothing to assert beyond "does not throw"; the decode tests below prove it
  assert.ok(true);
});

test("lz4-compressed IPC decodes byte-identical to the uncompressed stream", { skip: !HAS_REGISTRY }, () => {
  registerCompressionCodecs();
  const plain = tableFromIPC(fx("lz4-plain.arrows"));
  const comp = tableFromIPC(fx("lz4-compressed.arrows"));
  assert.equal(comp.numRows, plain.numRows);
  assert.ok(plain.numRows > 1000, `expected a big fixture, got ${plain.numRows}`);

  const [pS, pP, pV] = ["series_id", "period", "value"].map((c) => plain.getChild(c));
  const [cS, cP, cV] = ["series_id", "period", "value"].map((c) => comp.getChild(c));
  let mism = 0;
  for (let i = 0; i < plain.numRows; i++) {
    if (String(pS.get(i)) !== String(cS.get(i))) mism++;
    else if (String(pP.get(i)) !== String(cP.get(i))) mism++;
    else if (pV.get(i) !== cV.get(i)) mism++;
  }
  assert.equal(mism, 0, `${mism} value mismatches between compressed and uncompressed decode`);
});

test("compressed fixture is materially smaller on the wire", { skip: !HAS_REGISTRY }, () => {
  const plainBytes = fx("lz4-plain.arrows").length;
  const compBytes = fx("lz4-compressed.arrows").length;
  assert.ok(compBytes * 2 < plainBytes, `expected >2x smaller: plain ${plainBytes} vs comp ${compBytes}`);
});
