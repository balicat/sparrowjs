// Decode-error decoration — make upstream Arrow JS gaps name their fix.
// (Tester finding F5: the raw `Unrecognized type: "undefined" (24)` message
// told a DataFusion/ROAPI user nothing.)
import { ViewTranscodeUnsupported } from "./view-transcode.js";

// Arrow flatbuffer Type ids Arrow JS cannot decode yet (no View-type support)
const VIEW_TYPES: Record<number, string> = {
  23: "BinaryView",
  24: "Utf8View",
  25: "ListView",
  26: "LargeListView",
};

const SERVER_FIX =
  `DataFusion-family servers (ROAPI, InfluxDB 3) can serve classic types instead — ` +
  `set schema_force_view_types=false server-side ` +
  `(e.g. datafusion.execution.parquet.schema_force_view_types=false).`;

/**
 * Wrap a decode error with actionable guidance when it is the known
 * Arrow-JS-has-no-View-types upstream gap; pass anything else through.
 * Flat Utf8View/BinaryView columns never get here — the client transcodes
 * them (view-transcode.ts); this covers the combinations it refuses.
 */
export function decorateDecodeError(e: unknown): unknown {
  if (e instanceof ViewTranscodeUnsupported) {
    const wrapped = new Error(
      `decode: sparrowJS transcodes flat Utf8View/BinaryView columns automatically, ` +
        `but this stream needs more: ${e.message}. ${SERVER_FIX} Original: ${e.message}`,
    );
    (wrapped as Error & { cause?: unknown }).cause = e;
    return wrapped;
  }
  const msg = String((e as Error)?.message ?? e);
  const m = msg.match(/Unrecognized type[^(]*\((\d+)\)/);
  if (!m) return e;
  const id = Number(m[1]);
  const name = VIEW_TYPES[id] ?? `Arrow type ${id}`;
  const wrapped = new Error(
    `decode: the server sent ${name}, which Arrow JS cannot decode yet ` +
      `(View types are an upstream gap in apache-arrow JS). ${SERVER_FIX} ` +
      `Original: ${msg}`,
  );
  (wrapped as Error & { cause?: unknown }).cause = e;
  return wrapped;
}
