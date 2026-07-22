// Public types for sparrowjs.
import type { RecordBatch, Schema, Table } from "apache-arrow";

/** How int64/uint64 columns reach JavaScript. See docs/api-m1.md §bigint. */
export type BigIntMode = "bigint" | "number" | "string";

export interface ConnectOptions {
  /** gRPC-web edge in front of the Flight server (Envoy grpc_web / Traefik grpcWeb). */
  endpoint: string;
  /** Basic bootstrap — many servers mint a Bearer from this and the client adopts it. */
  user?: string;
  pass?: string;
  /** Bring your own token instead of Basic. */
  auth?: { bearer: string };
  /** Extra headers on every call (e.g. { database: "x" } for InfluxDB 3). */
  headers?: Record<string, string>;
  /** Default int64 handling for this client. Default "bigint" (lossless). */
  bigIntMode?: BigIntMode;
}

export interface QueryOptions {
  /** Fires per Arrow RecordBatch as it decodes off the wire. */
  onBatch?: (batch: RecordBatch, index: number, msSinceStart: number) => void;
  /** Override the client-level bigIntMode for this query. */
  bigIntMode?: BigIntMode;
  /**
   * Set false to force the planned 2-RTT path (GetFlightInfo → DoGet) even
   * when the server advertises the "sql" direct-ticket template. Default:
   * query() auto-routes 1-RTT where the template is advertised.
   */
  direct?: boolean;
}

/** sparrowCLI --stats, minus the terminal. */
export interface QueryStats {
  /** Bootstrap share of this call (0 on a warm client). */
  authMs: number;
  /** GetFlightInfo round trip. */
  planMs: number;
  /** "direct" = 1-RTT ticket straight to DoGet (pull(), doGet(), or query()
   *  auto-routed via the advertised "sql" template); "planned" = 2-RTT. */
  route: "direct" | "planned";
  /** Plan done → first decoded batch. */
  firstBatchMs: number;
  /** First batch → last batch. */
  streamMs: number;
  totalMs: number;
  rows: number;
  batches: number;
  /** FlightData frames as received (header + body, re-encapsulated). */
  wireBytes: number;
  /** Rows over the TRANSFER window (first→last batch) — first-byte latency is
   *  not a rate. Single-batch results fall back to the DoGet window (latency
   *  and transfer are indistinguishable for one batch). */
  rowsPerSec: number;
  /** Wire bits over the same transfer window as rowsPerSec. */
  mbitPerSec: number;
}

export interface QueryResult {
  table: Table;
  stats: QueryStats;
}

/** Decoded GetSqlInfo — cached at connect(), so this is synchronous. */
export interface Capabilities {
  vendorName?: string;
  vendorVersion?: string;
  arrowVersion?: string;
  readOnly?: boolean;
  sql?: boolean;
  /** Server consumes Substrait plans (SqlInfo code 5). */
  substrait?: boolean;
  transactions?: number;
  cancel?: boolean;
  /**
   * Client-constructed ticket templates the server advertises for 1-RTT
   * pulls (Sparrow vendor SqlInfo code 10100), or undefined if the server
   * doesn't advertise them. An empty array means "advertises the extension
   * but offers no templates". `pull()` uses this to fail fast.
   */
  directTickets?: TicketTemplate[];
  /** Every SqlInfo entry the server sent, by code. */
  raw: Map<number, unknown>;
}

/** One advertised direct-ticket template (see docs/api-m1.md · TICKETS.md). */
export interface TicketTemplate {
  /** template id, e.g. "series-pull" */
  id: string;
  doc?: string;
  /** field → type hint, e.g. { series: "string[]", start: "string?" } */
  ticket: Record<string, string>;
  /** result columns as "name:type" strings */
  result?: string[];
}

export interface TableInfo {
  catalog: string | null;
  dbSchema: string | null;
  name: string;
  /** e.g. "BASE TABLE", "VIEW", "MACRO" (Sparrow advertises search_meta this way). */
  type: string;
}

export type { RecordBatch, Schema, Table };
