// sparrowjs — Arrow Flight (SQL) in the browser over gRPC-web.
export { connect, FlightClient } from "./lib/client.js";
export type { DescriptorInit, TicketInit } from "./lib/client.js";
export { registerCompressionCodecs } from "./lib/compression.js";
export { QueryStream } from "./lib/query-stream.js";
export { QueryBuilder } from "./lib/builder.js";
export type {
  BigIntMode,
  Capabilities,
  ConnectOptions,
  QueryOptions,
  QueryResult,
  QueryStats,
  TableInfo,
} from "./lib/types.js";
