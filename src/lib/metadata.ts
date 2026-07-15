// GetTables decode + schema-bytes handling. GetTables is the portable
// discovery path (validated across GizmoSQL / InfluxDB 3 / Dremio / Sparrow —
// see dialect-compat). The result is itself an Arrow table we decode with our
// own pipeline.
import { Schema, Table, tableFromIPC } from "apache-arrow";
import { encapsulate, EOS } from "./ipc.js";
import type { TableInfo } from "./types.js";

export function tableInfosFrom(t: Table): TableInfo[] {
  const catalog = t.getChild("catalog_name");
  const dbSchema = t.getChild("db_schema_name");
  const name = t.getChild("table_name");
  const type = t.getChild("table_type");
  const out: TableInfo[] = [];
  for (let i = 0; i < t.numRows; i++) {
    out.push({
      catalog: (catalog?.get(i) as string | null) ?? null,
      dbSchema: (dbSchema?.get(i) as string | null) ?? null,
      name: String(name?.get(i) ?? ""),
      type: String(type?.get(i) ?? ""),
    });
  }
  return out;
}

/** Exact-name row's serialized schema from a GetTables(include_schema) result.
 *  (The filter pattern is SQL LIKE, so `series_data`'s underscores can match
 *  extra rows — match the literal name here.) */
export function schemaBytesFor(t: Table, tableName: string): Uint8Array | undefined {
  const name = t.getChild("table_name");
  const schemaCol = t.getChild("table_schema");
  if (!name || !schemaCol) return undefined;
  for (let i = 0; i < t.numRows; i++) {
    if (String(name.get(i)) === tableName) {
      const bytes = schemaCol.get(i) as Uint8Array | null;
      if (bytes && bytes.length) return bytes;
    }
  }
  return undefined;
}

/** Serialized-schema bytes → arrow Schema. Producers differ: pyarrow ships
 *  the full encapsulation (continuation marker + length + Message), arrow-rs
 *  (FlightInfo.schema) ships the bare Message flatbuffer. Normalize both. */
export function decodeSchemaBytes(bytes: Uint8Array): Schema {
  let enc = bytes;
  const dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (bytes.length < 8 || dv.getUint32(0, true) !== 0xffffffff) {
    enc = encapsulate(bytes, new Uint8Array(0)); // bare Message → wrap it
  }
  try {
    return tableFromIPC(enc).schema;
  } catch {
    const withEos = new Uint8Array(enc.length + EOS.length);
    withEos.set(enc, 0);
    withEos.set(EOS, enc.length);
    return tableFromIPC(withEos).schema;
  }
}

/** Double-quote an identifier unless it is a plain (possibly dotted) name. */
export function quoteIdent(name: string): string {
  if (/^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$/.test(name)) return name;
  return `"${name.replace(/"/g, '""')}"`;
}
