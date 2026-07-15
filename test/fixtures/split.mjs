// Shared: split a captured fixture file back into encapsulated IPC messages.
// Message envelope: [FFFFFFFF][metaLen u32][meta (padded)][body]; body length
// comes from the flatbuffer Message.bodyLength (root table slot 3).
export function splitFixture(bytes) {
  const out = [];
  let pos = 0;
  const dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  while (pos + 8 <= bytes.length) {
    if (dv.getUint32(pos, true) !== 0xffffffff) throw new Error(`bad marker at ${pos}`);
    const metaLen = dv.getUint32(pos + 4, true);
    if (metaLen === 0) {
      out.push(bytes.subarray(pos, pos + 8)); // EOS
      pos += 8;
      continue;
    }
    // read Message.bodyLength: root table, vtable slot 3
    const meta = pos + 8;
    const root = meta + dv.getUint32(meta, true);
    const vt = root - dv.getInt32(root, true);
    const vtSize = dv.getUint16(vt, true);
    let bodyLen = 0;
    if (4 + 3 * 2 < vtSize) {
      const off = dv.getUint16(vt + 4 + 3 * 2, true);
      if (off) bodyLen = Number(dv.getBigInt64(root + off, true));
    }
    const padded = (bodyLen + 7) & ~7;
    out.push(bytes.subarray(pos, pos + 8 + metaLen + padded));
    pos += 8 + metaLen + padded;
  }
  return out;
}
