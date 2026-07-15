// int64 handling — more acute in JS than anywhere: Arrow decodes int64 as
// BigInt64Array (lossless), but JSON.stringify throws on BigInt and most
// chart libraries choke. bigIntMode converts on the convenience paths only;
// "bigint" (the default) stays zero-copy.
import { DataType, Float64, RecordBatch, Table, Utf8, Vector, vectorFromArray } from "apache-arrow";
import type { BigIntMode } from "./types.js";

const MAX_SAFE = 9007199254740992n; // 2^53

function is64BitInt(t: DataType): boolean {
  return DataType.isInt(t) && (t as { bitWidth?: number }).bitWidth === 64;
}

function toNumberVector(v: Vector, name: string): Vector {
  const out: (number | null)[] = new Array(v.length);
  for (let i = 0; i < v.length; i++) {
    const x = v.get(i);
    if (x == null) {
      out[i] = null;
      continue;
    }
    const b = typeof x === "bigint" ? x : BigInt(x);
    if (b > MAX_SAFE || b < -MAX_SAFE) {
      throw new RangeError(
        `bigIntMode "number": column "${name}" value ${b} exceeds 2^53 — ` +
          `use bigIntMode "bigint" or "string" to keep precision`,
      );
    }
    out[i] = Number(b);
  }
  return vectorFromArray(out, new Float64());
}

function toStringVector(v: Vector): Vector {
  const out: (string | null)[] = new Array(v.length);
  for (let i = 0; i < v.length; i++) {
    const x = v.get(i);
    out[i] = x == null ? null : String(x);
  }
  return vectorFromArray(out, new Utf8());
}

/**
 * Convert every int64/uint64 column of a batch per the mode. Returns the
 * batch untouched when there is nothing to convert (the common case — and
 * the zero-copy guarantee for "bigint" mode is that this is never called).
 */
export function convertBatch(batch: RecordBatch, mode: Exclude<BigIntMode, "bigint">): RecordBatch {
  const fields = batch.schema.fields;
  let needed = false;
  for (const f of fields) {
    if (is64BitInt(f.type)) {
      needed = true;
      break;
    }
  }
  if (!needed) return batch;

  const cols: Record<string, Vector> = {};
  for (let i = 0; i < fields.length; i++) {
    const f = fields[i];
    const child = batch.getChildAt(i) as Vector | null;
    if (child == null) continue;
    cols[f.name] = is64BitInt(f.type)
      ? mode === "number"
        ? toNumberVector(child, f.name)
        : toStringVector(child)
      : child;
  }
  return new Table(cols).batches[0];
}
