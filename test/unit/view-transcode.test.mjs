// Offline tests against REAL DataFusion (roapi) bytes — Arrow JS cannot
// produce Utf8View itself, so the fixtures are captured from the live server
// (test/fixtures/capture.mjs regenerates them).
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { tableFromIPC } from "apache-arrow";
import { viewTranscode } from "../../dist/lib/view-transcode.js";
import { splitFixture } from "../fixtures/split.mjs";

async function transcodeFixture(name) {
  const bytes = new Uint8Array(readFileSync(new URL(`../fixtures/${name}`, import.meta.url)));
  const msgs = splitFixture(bytes);
  async function* src() {
    for (const m of msgs) yield m;
  }
  const chunks = [];
  for await (const c of viewTranscode(src())) chunks.push(c);
  const total = chunks.reduce((n, c) => n + c.length, 0);
  const ipc = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) {
    ipc.set(c, off);
    off += c.length;
  }
  return tableFromIPC(ipc);
}

test("real roapi Utf8View column decodes to classic Utf8 with correct values", async () => {
  const t = await transcodeFixture("roapi-utf8view.bin");
  assert.equal(t.numRows, 5);
  assert.equal(String(t.schema.fields[0].type), "Utf8");
  const values = t.toArray().map((r) => r.series_id);
  // long strings (>12 bytes) exercise the variadic-buffer path
  assert.ok(values.includes("BH.Canada.Directional.Current"), values.join(","));
  assert.ok(values.every((v) => typeof v === "string" && v.length > 0));
});

test("mixed Utf8View + Float64 batch: layout walk preserves the non-view column", async () => {
  const t = await transcodeFixture("roapi-utf8view-mixed.bin");
  assert.equal(t.numRows, 300);
  const names = t.schema.fields.map((f) => `${f.name}:${f.type}`);
  assert.deepEqual(names, ["series_id:Utf8", "value:Float64"]);
  const rows = t.toArray();
  assert.ok(rows.every((r) => typeof r.series_id === "string"));
  assert.ok(rows.every((r) => typeof r.value === "number" && Number.isFinite(r.value)));
});

test("streams without View columns pass through byte-identical", async () => {
  // the transcoded output of a classic stream must be the input itself
  const bytes = new Uint8Array(readFileSync(new URL("../fixtures/roapi-utf8view.bin", import.meta.url)));
  const msgs = splitFixture(bytes);
  // first transcode → classic stream; run THAT through viewTranscode again
  async function* src() {
    for (const m of msgs) yield m;
  }
  const pass1 = [];
  for await (const c of viewTranscode(src())) pass1.push(c);
  async function* src2() {
    for (const m of pass1) yield m;
  }
  const pass2 = [];
  for await (const c of viewTranscode(src2())) pass2.push(c);
  assert.equal(pass1.length, pass2.length);
  for (let i = 0; i < pass1.length; i++) {
    assert.equal(pass2[i], pass1[i], `message ${i} not passed through by reference`);
  }
});
