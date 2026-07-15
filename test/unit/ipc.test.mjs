import { test } from "node:test";
import assert from "node:assert/strict";
import { encapsulate, pad8, EOS } from "../../dist/lib/ipc.js";

test("pad8", () => {
  assert.deepEqual([0, 1, 7, 8, 9].map(pad8), [0, 8, 8, 8, 16]);
});

test("encapsulate layout: marker, padded lengths, header+body placement", () => {
  const header = Uint8Array.from([1, 2, 3]); // pads to 8
  const body = Uint8Array.from([9, 9, 9, 9, 9]); // pads to 8
  const buf = encapsulate(header, body);
  assert.equal(buf.length, 8 + 8 + 8);
  const dv = new DataView(buf.buffer);
  assert.equal(dv.getUint32(0, true), 0xffffffff); // continuation marker
  assert.equal(dv.getUint32(4, true), 8); // padded metadata length
  assert.deepEqual([...buf.slice(8, 11)], [1, 2, 3]);
  assert.deepEqual([...buf.slice(11, 16)], [0, 0, 0, 0, 0]); // header padding
  assert.deepEqual([...buf.slice(16, 21)], [9, 9, 9, 9, 9]);
});

test("EOS is the 8-byte end-of-stream marker", () => {
  assert.deepEqual([...EOS], [0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0]);
});
