import { test } from "node:test";
import assert from "node:assert/strict";
import { decorateDecodeError } from "../../dist/lib/errors.js";

test("Utf8View (24) decode error names the type and the server-side fix", () => {
  const raw = new Error('Unrecognized type: "undefined" (24)');
  const out = decorateDecodeError(raw);
  assert.notEqual(out, raw);
  assert.match(out.message, /Utf8View/);
  assert.match(out.message, /schema_force_view_types=false/);
  assert.equal(out.cause, raw);
});

test("BinaryView (23) maps too", () => {
  const out = decorateDecodeError(new Error('Unrecognized type: "undefined" (23)'));
  assert.match(out.message, /BinaryView/);
});

test("unrelated errors pass through untouched", () => {
  const raw = new Error("some other failure");
  assert.equal(decorateDecodeError(raw), raw);
});
