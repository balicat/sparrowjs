import { test } from "node:test";
import assert from "node:assert/strict";
import { Int64, Utf8, tableFromArrays, vectorFromArray, Table } from "apache-arrow";
import { convertBatch } from "../../dist/lib/bigint.js";

function batchWith(values) {
  const t = new Table({
    big: vectorFromArray(values, new Int64()),
    label: vectorFromArray(values.map((v) => (v == null ? null : `v${v}`)), new Utf8()),
  });
  return t.batches[0];
}

test('"number" converts small int64 to Float64, preserves nulls and other columns', () => {
  const out = convertBatch(batchWith([1n, null, -42n]), "number");
  const big = out.getChild("big");
  assert.equal(big.type.toString(), "Float64");
  assert.deepEqual([big.get(0), big.get(1), big.get(2)], [1, null, -42]);
  assert.equal(out.getChild("label").get(2), "v-42");
});

test('"number" throws loud past 2^53 — never silent precision loss', () => {
  assert.throws(() => convertBatch(batchWith([9007199254740993n]), "number"), RangeError);
});

test('"string" stringifies, keeps precision', () => {
  const out = convertBatch(batchWith([9007199254740993n, null]), "string");
  const big = out.getChild("big");
  assert.equal(big.get(0), "9007199254740993");
  assert.equal(big.get(1), null);
});

test("batch without int64 columns passes through untouched (same object)", () => {
  const t = tableFromArrays({ x: Float64Array.from([1.5, 2.5]) });
  const b = t.batches[0];
  assert.equal(convertBatch(b, "number"), b);
});
