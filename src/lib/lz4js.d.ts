// Minimal ambient types for lz4js (ships no types). Browser LZ4 *Frame*
// implementation — the codec apache-arrow's compressionRegistry needs for
// LZ4_FRAME. See compression.ts.
declare module "lz4js" {
  export function compress(data: Uint8Array): Uint8Array;
  export function decompress(data: Uint8Array): Uint8Array;
}
