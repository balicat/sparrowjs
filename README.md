# @sparrowjs/flight

Arrow Flight. In the browser. Finally.

A gRPC-web + Apache Arrow JS client for any Arrow Flight SQL server — record batches
streaming into the page, zero copies, no REST gateway rewriting your data as text.

Part of the [Sparrow](https://sparrowflight.io) family · spec: [sparrowflight.io/js](https://sparrowflight.io/js)

## Status: M0 (proof of the pipeline)

`src/m0.mjs` runs the full chain in Node using the *browser* transport
(`@connectrpc/connect-web`, fetch + ReadableStream):

```
✓ auth bootstrap: Bearer adopted in 25 ms
✓ GetFlightInfo: 1 endpoint(s) in 34 ms
✓ DoGet: 2 FlightData frames streamed
✓ Arrow JS decoded: 5 rows × 2 cols in 46 ms total
```

- Flight + Flight SQL stubs generated from the Apache protos (`buf` + `protoc-gen-es`)
- `CommandStatementQuery` → `GetFlightInfo` → server-streaming `DoGet` over **gRPC-web**
  (through Envoy's `grpc_web` filter)
- FlightData frames reassembled into an Arrow IPC stream by hand
  (continuation marker + padded header + padded body) and decoded with `apache-arrow`
- Auth: Basic bootstrap → **Bearer adopted from response headers** (the same silent
  trick the ADBC driver does; sessions bind to the Bearer on GizmoSQL-style servers)

## Run it

```
npm install
npm run gen        # regenerate proto stubs
SPARROW_ENDPOINT=http://host:8890 node src/m0.mjs "SELECT 42 AS answer"
```

Requires a gRPC-web edge in front of the Flight server (Envoy config in the spec).

## Next (M1)

Proper `FlightClient` API, multi-batch/dictionary IPC handling, browser smoke test,
error taxonomy. See `sparrowJS/spec.md` in the Sparrow project folder.
