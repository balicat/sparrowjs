// Register decompression codecs so sparrowJS reads COMPRESSED Arrow IPC
// streams in the browser. apache-arrow ≤21.1 hard-throws on a compressed
// record batch ("Record batch compression not implemented"); 21.2 added a
// `compressionRegistry` hook (apache/arrow-js PR #14) where the caller
// supplies the codec — decode-only, exactly our case (the server compresses,
// the browser reads).
//
// We register lz4js for LZ4_FRAME — the format Arrow servers emit (pyarrow
// `pa.Codec("lz4")`, DuckDB, GizmoSQL all produce LZ4 *Frame*, magic
// 0x184D2204). lz4js decodes the frame format; lz4-wasm does NOT (raw blocks
// only) — a trap the upstream PR calls out and we verified against real bytes.
//
// Feature-detected: a no-op on older apache-arrow (the registry doesn't
// exist), so sparrowJS stays installable across its whole `>=17` peer range
// and decodes compression only where the installed Arrow supports it. Over a
// bandwidth-limited link this is a ~3.5× smaller wire for the same rows.
import * as arrow from "apache-arrow";
import * as lz4js from "lz4js";

let registered = false;
let supported = false;

/**
 * Idempotent. Registers the LZ4_FRAME decompressor when the installed
 * apache-arrow exposes the compression registry (≥21.2); no-op otherwise.
 * Called from the FlightClient constructor, so it "just works"; also exported
 * for callers who construct Arrow readers directly.
 */
export function registerCompressionCodecs(): void {
  if (registered) return;
  registered = true;
  const a = arrow as unknown as Record<string, unknown>;
  const reg = a["compressionRegistry"] as
    | { set(type: number, codec: unknown): void }
    | undefined;
  const CT = a["CompressionType"] as Record<string, number> | undefined;
  if (!reg || typeof reg.set !== "function" || !CT || typeof CT["LZ4_FRAME"] !== "number") {
    return; // apache-arrow < 21.2 — no compression hook to register into
  }
  reg.set(CT["LZ4_FRAME"], {
    encode: (d: Uint8Array): Uint8Array => lz4js.compress(d),
    decode: (d: Uint8Array): Uint8Array => lz4js.decompress(d),
  });
  supported = true;
}

/**
 * Codec names this client can DECODE, for the `accept_compression` field of a
 * Sparrow JSON ticket. Empty on apache-arrow < 21.2 — so the server sees no
 * accepted codec and serves uncompressed IPC, which older Arrow can read.
 * The server compresses only for a mode the client lists here.
 */
export function acceptedCompression(): string[] {
  registerCompressionCodecs();
  return supported ? ["lz4"] : [];
}
