// Utf8View/BinaryView → classic Utf8/Binary, client-side, at the IPC layer.
//
// Arrow JS cannot decode View types (upstream gap); DataFusion-family servers
// emit them for every parquet-sourced string column. This module rewrites the
// stream BEFORE Arrow JS sees it — the same transformation
// `schema_force_view_types=false` performs server-side, done in the client so
// sparrowJS works against DataFusion servers the user does not control.
//
// Scope (v1): flat top-level Utf8View/BinaryView columns. Nested views,
// dictionary-encoded views, and compressed bodies throw
// ViewTranscodeUnsupported (decorated upstream in errors.ts).
//
// Implementation: a minimal flatbuffer reader (vtable walk) for the two
// message shapes we touch, an in-place union-tag rewrite for the schema
// (Utf8View and Utf8 are both EMPTY flatbuffer tables — the union tag byte is
// the entire difference), and a from-scratch rebuild of each RecordBatch
// message, because buffer counts change: a view column's
// [validity, views, data₀..dataₖ] becomes [validity, offsets, data].

// ── Arrow flatbuffer constants ─────────────────────────────────────────────
const HEADER_SCHEMA = 1;
const HEADER_DICTIONARY = 2;
const HEADER_RECORD_BATCH = 3;

// Type union tags (Schema.fbs)
const T_NULL = 1;
const T_BINARY = 4;
const T_UTF8 = 5;
const T_BINARY_VIEW = 23;
const T_UTF8_VIEW = 24;

/** buffers per flat column, by type tag; -1 = variadic (view), -2 = unsupported here */
function bufferCount(tag: number): number {
  switch (tag) {
    case T_NULL:
      return 0;
    case 2: // Int
    case 3: // FloatingPoint
    case 6: // Bool
    case 7: // Decimal
    case 8: // Date
    case 9: // Time
    case 10: // Timestamp
    case 11: // Interval
    case 15: // FixedSizeBinary
    case 18: // Duration
      return 2; // validity + data
    case T_BINARY:
    case T_UTF8:
    case 19: // LargeBinary
    case 20: // LargeUtf8
      return 3; // validity + offsets + data
    case T_BINARY_VIEW:
    case T_UTF8_VIEW:
      return -1;
    default:
      return -2; // nested/exotic — cannot walk the flat buffer layout
  }
}

export class ViewTranscodeUnsupported extends Error {}

// ── minimal flatbuffer reader ──────────────────────────────────────────────
class FBReader {
  readonly dv: DataView;
  constructor(readonly buf: Uint8Array) {
    this.dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  }
  root(): number {
    return this.dv.getUint32(0, true);
  }
  /** absolute byte position of a table field's value, or 0 if absent */
  field(table: number, slot: number): number {
    const vt = table - this.dv.getInt32(table, true);
    const vtSize = this.dv.getUint16(vt, true);
    const entry = 4 + slot * 2;
    if (entry >= vtSize) return 0;
    const off = this.dv.getUint16(vt + entry, true);
    return off === 0 ? 0 : table + off;
  }
  u8(pos: number): number {
    return this.dv.getUint8(pos);
  }
  i16(pos: number): number {
    return this.dv.getInt16(pos, true);
  }
  i64(pos: number): number {
    return Number(this.dv.getBigInt64(pos, true));
  }
  /** follow a uoffset (table/vector/string reference) */
  indirect(pos: number): number {
    return pos + this.dv.getUint32(pos, true);
  }
  vectorLen(vecPos: number): number {
    return this.dv.getUint32(vecPos, true);
  }
  vectorData(vecPos: number): number {
    return vecPos + 4;
  }
}

// ── message envelope ───────────────────────────────────────────────────────

interface IpcMessage {
  /** flatbuffer Message bytes (the encapsulation's metadata part) */
  meta: Uint8Array;
  body: Uint8Array;
}

/** Split one encapsulated IPC message; returns null for EOS/undecodable. */
function splitEncapsulated(chunk: Uint8Array): IpcMessage | null {
  if (chunk.length < 8) return null;
  const dv = new DataView(chunk.buffer, chunk.byteOffset, chunk.byteLength);
  if (dv.getUint32(0, true) !== 0xffffffff) return null;
  const metaLen = dv.getUint32(4, true);
  if (metaLen === 0) return null; // EOS
  return { meta: chunk.subarray(8, 8 + metaLen), body: chunk.subarray(8 + metaLen) };
}

function pad8(n: number): number {
  return (n + 7) & ~7;
}

function encapsulate(meta: Uint8Array, body: Uint8Array): Uint8Array {
  const metaLen = pad8(meta.length);
  const out = new Uint8Array(8 + metaLen + pad8(body.length));
  const dv = new DataView(out.buffer);
  dv.setUint32(0, 0xffffffff, true);
  dv.setUint32(4, metaLen, true);
  out.set(meta, 8);
  out.set(body, 8 + metaLen);
  return out;
}

// ── schema analysis + in-place tag rewrite ─────────────────────────────────

interface SchemaPlan {
  /** type tag per top-level field, AFTER rewrite (views → classic) */
  tags: number[];
  /** indices of fields that were view-typed */
  viewCols: number[];
}

/**
 * Parse a Schema message; if any top-level field is Utf8View/BinaryView,
 * rewrite its union tag IN PLACE to Utf8/Binary and return the plan.
 * Returns null when the schema has no view columns (pure passthrough).
 */
function planSchema(r: FBReader, headerPos: number): SchemaPlan | null {
  const fieldsVecRef = r.field(headerPos, 1); // Schema.fields
  if (!fieldsVecRef) return null;
  const vec = r.indirect(fieldsVecRef);
  const n = r.vectorLen(vec);
  const tags: number[] = [];
  const viewCols: number[] = [];
  for (let i = 0; i < n; i++) {
    const field = r.indirect(r.vectorData(vec) + i * 4);
    const tagPos = r.field(field, 2); // Field.type_type
    const tag = tagPos ? r.u8(tagPos) : 0;
    if (tag === T_UTF8_VIEW || tag === T_BINARY_VIEW) {
      if (r.field(field, 4)) {
        // Field.dictionary
        throw new ViewTranscodeUnsupported("dictionary-encoded View column");
      }
      r.buf[tagPos] = tag === T_UTF8_VIEW ? T_UTF8 : T_BINARY;
      tags.push(r.buf[tagPos]);
      viewCols.push(i);
    } else {
      const childrenRef = r.field(field, 5);
      if (childrenRef && viewInChildren(r, childrenRef)) {
        throw new ViewTranscodeUnsupported("View type nested inside " + tagName(tag));
      }
      tags.push(tag);
    }
  }
  return viewCols.length ? { tags, viewCols } : null;
}

function viewInChildren(r: FBReader, childrenRef: number): boolean {
  const vec = r.indirect(childrenRef);
  const n = r.vectorLen(vec);
  for (let i = 0; i < n; i++) {
    const field = r.indirect(r.vectorData(vec) + i * 4);
    const tagPos = r.field(field, 2);
    const tag = tagPos ? r.u8(tagPos) : 0;
    if (tag === T_UTF8_VIEW || tag === T_BINARY_VIEW) return true;
    const kids = r.field(field, 5);
    if (kids && viewInChildren(r, kids)) return true;
  }
  return false;
}

function tagName(tag: number): string {
  return (
    (
      {
        12: "List",
        13: "Struct",
        14: "Union",
        16: "FixedSizeList",
        17: "Map",
        22: "RunEndEncoded",
        25: "ListView",
        26: "LargeListView",
      } as Record<number, string>
    )[tag] ?? `type ${tag}`
  );
}

// ── RecordBatch rebuild ────────────────────────────────────────────────────

function transcodeBatch(msg: IpcMessage, plan: SchemaPlan, version: number): Uint8Array {
  const r = new FBReader(msg.meta);
  const header = r.indirect(r.field(r.root(), 2));

  if (r.field(header, 3)) {
    throw new ViewTranscodeUnsupported("compressed IPC body + View types");
  }

  const lengthPos = r.field(header, 0);
  const rows = lengthPos ? r.i64(lengthPos) : 0;

  const nodesVec = r.indirect(r.field(header, 1));
  const nodeCount = r.vectorLen(nodesVec);
  if (nodeCount !== plan.tags.length) {
    throw new ViewTranscodeUnsupported(`nested layout (${nodeCount} nodes for ${plan.tags.length} fields)`);
  }
  const nodes: { length: number; nullCount: number }[] = [];
  for (let i = 0; i < nodeCount; i++) {
    const p = r.vectorData(nodesVec) + i * 16;
    nodes.push({ length: r.i64(p), nullCount: r.i64(p + 8) });
  }

  const buffersVec = r.indirect(r.field(header, 2));
  const bufMeta: { offset: number; length: number }[] = [];
  for (let i = 0, n = r.vectorLen(buffersVec); i < n; i++) {
    const p = r.vectorData(buffersVec) + i * 16;
    bufMeta.push({ offset: r.i64(p), length: r.i64(p + 8) });
  }

  const variadic: number[] = [];
  const variadicRef = r.field(header, 4);
  if (variadicRef) {
    const vec = r.indirect(variadicRef);
    for (let i = 0, n = r.vectorLen(vec); i < n; i++) variadic.push(r.i64(r.vectorData(vec) + i * 8));
  }

  // walk columns, consuming buffers per ORIGINAL layout, emitting classic
  const viewSet = new Set(plan.viewCols);
  const out: Uint8Array[] = [];
  let bi = 0;
  let vi = 0;
  const take = () => {
    const m = bufMeta[bi++];
    if (!m) throw new ViewTranscodeUnsupported("buffer list shorter than the schema layout expects");
    return msg.body.subarray(m.offset, m.offset + m.length);
  };
  for (let col = 0; col < plan.tags.length; col++) {
    if (viewSet.has(col)) {
      const validity = take();
      const views = take();
      const k = variadic[vi++] ?? 0;
      const data: Uint8Array[] = [];
      for (let j = 0; j < k; j++) data.push(take());
      const { offsets, bytes } = viewsToClassic(views, data, nodes[col].length);
      out.push(validity, offsets, bytes);
    } else {
      const n = bufferCount(plan.tags[col]);
      if (n === -2) {
        throw new ViewTranscodeUnsupported(tagName(plan.tags[col]) + " column alongside View columns");
      }
      for (let j = 0; j < n; j++) out.push(take());
    }
  }

  // reassemble: 8-aligned body + a fresh Message flatbuffer
  let bodyLen = 0;
  const finalMeta: { offset: number; length: number }[] = [];
  for (const b of out) {
    finalMeta.push({ offset: bodyLen, length: b.length });
    bodyLen += pad8(b.length);
  }
  const body = new Uint8Array(bodyLen);
  for (let i = 0; i < out.length; i++) body.set(out[i], finalMeta[i].offset);

  return encapsulate(buildRecordBatchMessage(version, rows, nodes, finalMeta, bodyLen), body);
}

/** decode 16-byte view structs into classic offsets+data */
function viewsToClassic(
  views: Uint8Array,
  data: Uint8Array[],
  rows: number,
): { offsets: Uint8Array; bytes: Uint8Array } {
  const dv = new DataView(views.buffer, views.byteOffset, views.byteLength);
  const offsets = new Int32Array(rows + 1);
  let total = 0;
  for (let i = 0; i < rows; i++) total += dv.getInt32(i * 16, true);
  if (total > 0x7fffffff) {
    throw new ViewTranscodeUnsupported(`column data exceeds 2 GiB (${total} bytes) — classic Utf8 cannot hold it`);
  }
  const bytes = new Uint8Array(total);
  let w = 0;
  for (let i = 0; i < rows; i++) {
    const base = i * 16;
    const len = dv.getInt32(base, true);
    if (len > 0) {
      if (len <= 12) {
        bytes.set(views.subarray(base + 4, base + 4 + len), w);
      } else {
        const bufIdx = dv.getInt32(base + 8, true);
        const off = dv.getInt32(base + 12, true);
        const src = data[bufIdx];
        if (!src) throw new ViewTranscodeUnsupported(`view references missing data buffer ${bufIdx}`);
        bytes.set(src.subarray(off, off + len), w);
      }
      w += len;
    }
    offsets[i + 1] = w;
  }
  return { offsets: new Uint8Array(offsets.buffer), bytes };
}

// ── minimal flatbuffer writer: Message{RecordBatch} only ───────────────────
// Front-to-back with precomputed positions — uoffsets point forward, so
// parents sit at lower addresses than the vectors they reference.
//
// Layout (all little-endian):
//   0   u32   root uoffset → Message table (16)
//   4   Message vtable  (12 B: vt_size=12, tbl_size=24, slots 4/6/8/16)
//   16  Message table   (24 B: soffset, version i16, header_type u8, pad,
//                        header u32 @+8, pad, bodyLength i64 @+16)
//   40  RecordBatch vtable (10 B: vt_size=10, tbl_size=24, slots 8/16/20)
//   50  pad → 56
//   56  RecordBatch table (24 B: soffset, pad, length i64 @+8 (abs 64, 8-aligned),
//                          nodes u32 @+16, buffers u32 @+20)
//   80  pad → nodes count @ 84, node structs (16 B each) @ 88 (8-aligned)
//   ..  pad → buffers count, buffer structs (16 B each), 8-aligned data
function buildRecordBatchMessage(
  version: number,
  rows: number,
  nodes: { length: number; nullCount: number }[],
  buffers: { offset: number; length: number }[],
  bodyLen: number,
): Uint8Array {
  const MSG_VT = 4;
  const MSG_TBL = 16;
  const RB_VT = 40;
  const RB_TBL = 56; // 8-aligned so its i64 field at +8 is 8-aligned

  let p = RB_TBL + 24; // 80
  const nodesCountPos = ((p + 4 + 7) & ~7) - 4; // struct data must be 8-aligned
  const nodesDataPos = nodesCountPos + 4;
  p = nodesDataPos + nodes.length * 16;
  const buffersCountPos = ((p + 4 + 7) & ~7) - 4;
  const buffersDataPos = buffersCountPos + 4;
  const total = buffersDataPos + buffers.length * 16;

  const out = new Uint8Array(total);
  const dv = new DataView(out.buffer);

  dv.setUint32(0, MSG_TBL, true); // root

  // Message vtable — slots: version(0), header_type(1), header(2), bodyLength(3)
  dv.setUint16(MSG_VT, 12, true);
  dv.setUint16(MSG_VT + 2, 24, true);
  dv.setUint16(MSG_VT + 4, 4, true);
  dv.setUint16(MSG_VT + 6, 6, true);
  dv.setUint16(MSG_VT + 8, 8, true);
  dv.setUint16(MSG_VT + 10, 16, true);

  // Message table
  dv.setInt32(MSG_TBL, MSG_TBL - MSG_VT, true);
  dv.setInt16(MSG_TBL + 4, version, true);
  dv.setUint8(MSG_TBL + 6, HEADER_RECORD_BATCH);
  dv.setUint32(MSG_TBL + 8, RB_TBL - (MSG_TBL + 8), true);
  dv.setBigInt64(MSG_TBL + 16, BigInt(bodyLen), true);

  // RecordBatch vtable — slots: length(0), nodes(1), buffers(2)
  dv.setUint16(RB_VT, 10, true);
  dv.setUint16(RB_VT + 2, 24, true);
  dv.setUint16(RB_VT + 4, 8, true);
  dv.setUint16(RB_VT + 6, 16, true);
  dv.setUint16(RB_VT + 8, 20, true);

  // RecordBatch table
  dv.setInt32(RB_TBL, RB_TBL - RB_VT, true);
  dv.setBigInt64(RB_TBL + 8, BigInt(rows), true);
  dv.setUint32(RB_TBL + 16, nodesCountPos - (RB_TBL + 16), true);
  dv.setUint32(RB_TBL + 20, buffersCountPos - (RB_TBL + 20), true);

  dv.setUint32(nodesCountPos, nodes.length, true);
  nodes.forEach((m, i) => {
    dv.setBigInt64(nodesDataPos + i * 16, BigInt(m.length), true);
    dv.setBigInt64(nodesDataPos + i * 16 + 8, BigInt(m.nullCount), true);
  });

  dv.setUint32(buffersCountPos, buffers.length, true);
  buffers.forEach((m, i) => {
    dv.setBigInt64(buffersDataPos + i * 16, BigInt(m.offset), true);
    dv.setBigInt64(buffersDataPos + i * 16 + 8, BigInt(m.length), true);
  });

  return out;
}

// ── the stream transform ───────────────────────────────────────────────────

/**
 * Wrap one endpoint's encapsulated-IPC stream. Watches the schema message;
 * when it declares flat View columns, rewrites the schema in place and
 * transcodes every RecordBatch. Zero-cost passthrough otherwise.
 */
export async function* viewTranscode(source: AsyncGenerator<Uint8Array>): AsyncGenerator<Uint8Array> {
  let plan: SchemaPlan | null = null;
  let sawSchema = false;
  let version = 4; // MetadataVersion V5; replaced by the schema message's value
  for await (const chunk of source) {
    const msg = splitEncapsulated(chunk);
    if (!msg) {
      yield chunk; // EOS or opaque — untouched
      continue;
    }
    const r = new FBReader(msg.meta);
    const root = r.root();
    const headerTypePos = r.field(root, 1);
    const headerType = headerTypePos ? r.u8(headerTypePos) : 0;
    const versionPos = r.field(root, 0);
    if (versionPos) version = r.i16(versionPos);

    if (!sawSchema && headerType === HEADER_SCHEMA) {
      sawSchema = true;
      const headerRef = r.field(root, 2);
      plan = headerRef ? planSchema(r, r.indirect(headerRef)) : null;
      yield chunk; // view tags were rewritten in place (chunk shares memory)
      continue;
    }
    if (plan && headerType === HEADER_RECORD_BATCH) {
      yield transcodeBatch(msg, plan, version);
      continue;
    }
    if (plan && headerType === HEADER_DICTIONARY) {
      throw new ViewTranscodeUnsupported("dictionary batches alongside View columns");
    }
    yield chunk;
  }
}
