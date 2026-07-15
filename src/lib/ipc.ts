// Flight IPC reassembly: FlightData frames → one Arrow IPC stream.
// The recipe proven in M0: continuation marker + padded header length +
// header + padded body, EOS marker at the end.

export function pad8(n: number): number {
  return (n + 7) & ~7;
}

/** Re-encapsulate one FlightData frame as an Arrow IPC stream message. */
export function encapsulate(dataHeader: Uint8Array, dataBody: Uint8Array): Uint8Array {
  const metaLen = pad8(dataHeader.length);
  const buf = new Uint8Array(8 + metaLen + pad8(dataBody.length));
  const dv = new DataView(buf.buffer);
  dv.setUint32(0, 0xffffffff, true); // continuation marker
  dv.setUint32(4, metaLen, true); // metadata length (incl. padding)
  buf.set(dataHeader, 8);
  buf.set(dataBody, 8 + metaLen);
  return buf;
}

/** Arrow IPC end-of-stream marker. */
export const EOS = new Uint8Array([0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0]);
