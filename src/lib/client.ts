// FlightClient — the M1 surface. connect() bootstraps eagerly: the GetSqlInfo
// call is session-less on every validated server, so one call (a) fails fast
// on bad endpoint/creds, (b) adopts the Bearer, (c) seeds capabilities().
import { Code, ConnectError, createClient, type Client } from "@connectrpc/connect";
import { createGrpcWebTransport } from "@connectrpc/connect-web";
import { create, toBinary, type DescMessage, type MessageShape } from "@bufbuild/protobuf";
import { AnySchema, anyPack } from "@bufbuild/protobuf/wkt";

import {
  FlightDescriptorSchema,
  FlightDescriptor_DescriptorType,
  FlightService,
  TicketSchema,
  type FlightDescriptor,
  type FlightInfo,
  type Ticket,
} from "../gen/Flight_pb.js";
import {
  CommandGetSqlInfoSchema,
  CommandGetTablesSchema,
  CommandStatementQuerySchema,
} from "../gen/FlightSql_pb.js";

import { AuthState } from "./auth.js";
import { QueryBuilder } from "./builder.js";
import { decodeSqlInfo, EMPTY_CAPABILITIES } from "./capabilities.js";
import { encapsulate, EOS } from "./ipc.js";
import { decodeSchemaBytes, quoteIdent, schemaBytesFor, tableInfosFrom } from "./metadata.js";
import { Marks, QueryStream } from "./query-stream.js";
import type {
  BigIntMode,
  Capabilities,
  ConnectOptions,
  QueryOptions,
  Schema,
  TableInfo,
} from "./types.js";

/** Escape hatch descriptor: a protobuf FlightDescriptor, a raw command, or a path. */
export type DescriptorInit = FlightDescriptor | { cmd: Uint8Array } | { path: string[] };

/** Escape hatch ticket: the protobuf Ticket or its raw bytes (JSON-ticket servers). */
export type TicketInit = Ticket | Uint8Array;

function descFor<S extends DescMessage>(schema: S, msg: MessageShape<S>): FlightDescriptor {
  return create(FlightDescriptorSchema, {
    type: FlightDescriptor_DescriptorType.CMD,
    cmd: toBinary(AnySchema, anyPack(schema, msg)),
  });
}

/** Bootstrap failures that mean the endpoint/creds are wrong — rethrow from
 *  connect(). Anything else (Unimplemented etc.) means "server has no
 *  GetSqlInfo"; we continue with empty capabilities. */
function isFatalBootstrapError(e: unknown): boolean {
  if (e instanceof ConnectError) {
    return (
      e.code === Code.Unauthenticated ||
      e.code === Code.PermissionDenied ||
      e.code === Code.Unavailable ||
      e.code === Code.DeadlineExceeded ||
      e.code === Code.Unknown
    );
  }
  return true; // fetch-level failure: bad URL, DNS, CORS
}

export class FlightClient {
  readonly #fc: Client<typeof FlightService>;
  readonly #auth: AuthState;
  readonly #mode: BigIntMode;
  #caps: Capabilities = EMPTY_CAPABILITIES;
  #bootstrap: Promise<void> | undefined;

  constructor(opts: ConnectOptions) {
    const transport = createGrpcWebTransport({ baseUrl: opts.endpoint });
    this.#fc = createClient(FlightService, transport);
    this.#auth = new AuthState({
      user: opts.user,
      pass: opts.pass,
      bearer: opts.auth?.bearer,
      headers: opts.headers,
    });
    this.#mode = opts.bigIntMode ?? "bigint";
  }

  // ── bootstrap ──────────────────────────────────────────────────────────

  /** Single-flight: concurrent first queries share one bootstrap, or the
   *  second Basic call races the first's Bearer adoption mid-flight. */
  bootstrap(): Promise<void> {
    this.#bootstrap ??= this.#doBootstrap();
    return this.#bootstrap;
  }

  async #doBootstrap(): Promise<void> {
    const desc = descFor(CommandGetSqlInfoSchema, create(CommandGetSqlInfoSchema, { info: [] }));
    let info: FlightInfo;
    try {
      info = await this.#fc.getFlightInfo(desc, this.#auth.callOptions());
    } catch (e) {
      if (isFatalBootstrapError(e)) throw e;
      return; // no GetSqlInfo on this server — Bearer adopts on first query
    }
    try {
      const stream = new QueryStream((marks, signal) => this.#endpoints(info, marks, signal));
      const { table } = await stream;
      this.#caps = decodeSqlInfo(table);
    } catch {
      // GetFlightInfo worked (auth is fine) but the DoGet leg failed —
      // capabilities stay empty, queries proceed
    }
  }

  /** Cached GetSqlInfo decode — synchronous, seeded at connect(). */
  capabilities(): Capabilities {
    return this.#caps;
  }

  // ── the pump machinery ─────────────────────────────────────────────────

  async *#endpoints(
    info: FlightInfo,
    marks: Marks,
    signal: AbortSignal,
  ): AsyncGenerator<AsyncGenerator<Uint8Array>> {
    for (const ep of info.endpoint) {
      if (!ep.ticket) continue;
      yield this.#frames(ep.ticket, marks, signal);
    }
  }

  async *#frames(ticket: Ticket, marks: Marks, signal: AbortSignal): AsyncGenerator<Uint8Array> {
    for await (const fd of this.#fc.doGet(ticket, this.#auth.callOptions(signal))) {
      const chunk = encapsulate(fd.dataHeader, fd.dataBody);
      marks.wireBytes += chunk.length;
      yield chunk;
    }
    yield EOS;
  }

  #commandStream(desc: FlightDescriptor, opts: QueryOptions = {}): QueryStream {
    const pump = async function* (
      this: FlightClient,
      marks: Marks,
      signal: AbortSignal,
    ): AsyncGenerator<AsyncGenerator<Uint8Array>> {
      const tA = performance.now();
      await this.bootstrap();
      marks.authMs = performance.now() - tA;
      const tP = performance.now();
      const info = await this.#fc.getFlightInfo(desc, this.#auth.callOptions(signal));
      marks.planMs = performance.now() - tP;
      marks.planDone();
      yield* this.#endpoints(info, marks, signal);
    }.bind(this);
    return new QueryStream(pump, opts, this.#mode);
  }

  // ── the everyday path ──────────────────────────────────────────────────

  /** Flight SQL statement → QueryStream (async-iterate it, or await it). */
  query(sql: string, opts?: QueryOptions): QueryStream {
    return this.#commandStream(
      descFor(CommandStatementQuerySchema, create(CommandStatementQuerySchema, { query: sql })),
      opts,
    );
  }

  /** Typed query builder: from("t").select(...).where(...).limit(n).query() */
  from(table: string): QueryBuilder {
    return new QueryBuilder(this, table);
  }

  // ── metadata (Tier 2) ──────────────────────────────────────────────────

  /** GetTables — the portable discovery path. Includes MACRO rows. */
  async tables(): Promise<TableInfo[]> {
    const desc = descFor(
      CommandGetTablesSchema,
      create(CommandGetTablesSchema, { includeSchema: false }),
    );
    const { table } = await this.#commandStream(desc);
    return tableInfosFrom(table);
  }

  /** Arrow schema of one table: GetTables(include_schema), LIMIT-0 fallback. */
  async schema(tableName: string): Promise<Schema> {
    try {
      const desc = descFor(
        CommandGetTablesSchema,
        create(CommandGetTablesSchema, { tableNameFilterPattern: tableName, includeSchema: true }),
      );
      const { table } = await this.#commandStream(desc);
      const bytes = schemaBytesFor(table, tableName);
      if (bytes) return decodeSchemaBytes(bytes);
    } catch {
      // fall through to the probe
    }
    const { table } = await this.query(`SELECT * FROM ${quoteIdent(tableName)} LIMIT 0`);
    return table.schema;
  }

  // ── raw Flight escape hatches (any Flight server, not just SQL) ────────

  async getFlightInfo(desc: DescriptorInit): Promise<FlightInfo> {
    await this.bootstrap();
    return this.#fc.getFlightInfo(this.#descriptor(desc), this.#auth.callOptions());
  }

  /** DoGet a ticket through the same reassembly + decode pipeline. */
  doGet(ticket: TicketInit, opts?: QueryOptions): QueryStream {
    const t =
      ticket instanceof Uint8Array ? create(TicketSchema, { ticket }) : ticket;
    const pump = async function* (
      this: FlightClient,
      marks: Marks,
      signal: AbortSignal,
    ): AsyncGenerator<AsyncGenerator<Uint8Array>> {
      const tA = performance.now();
      await this.bootstrap();
      marks.authMs = performance.now() - tA;
      marks.planDone();
      yield this.#frames(t, marks, signal);
    }.bind(this);
    return new QueryStream(pump, opts, this.#mode);
  }

  #descriptor(init: DescriptorInit): FlightDescriptor {
    if ("$typeName" in init) return init as FlightDescriptor;
    if ("cmd" in init) {
      return create(FlightDescriptorSchema, {
        type: FlightDescriptor_DescriptorType.CMD,
        cmd: init.cmd,
      });
    }
    return create(FlightDescriptorSchema, {
      type: FlightDescriptor_DescriptorType.PATH,
      path: init.path,
    });
  }
}

/** Connect eagerly: bad endpoint or creds fail HERE, not on the first query.
 *  The same round trip adopts the Bearer and seeds capabilities(). */
export async function connect(opts: ConnectOptions): Promise<FlightClient> {
  const client = new FlightClient(opts);
  await client.bootstrap();
  return client;
}
