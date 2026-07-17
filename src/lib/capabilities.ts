// GetSqlInfo decode — the sparrowCLI conformance-card logic, portable subset.
// The result table is (info_name: uint32, value: dense union); Arrow JS
// resolves union children on .get(), so values arrive unwrapped.
import type { Table } from "apache-arrow";
import type { Capabilities, TicketTemplate } from "./types.js";

// Flight SQL standard SqlInfo codes (the ones worth first-classing)
const SERVER_NAME = 0;
const SERVER_VERSION = 1;
const SERVER_ARROW_VERSION = 2;
const SERVER_READ_ONLY = 3;
const SERVER_SQL = 4;
const SERVER_SUBSTRAIT = 5;
const SERVER_TRANSACTION = 8;
const SERVER_CANCEL = 9;
// Sparrow vendor extension: JSON contract of client-constructed ticket
// templates for 1-RTT pulls (see TICKETS.md).
const SPARROW_DIRECT_TICKETS = 10100;

function decodeDirectTickets(v: unknown): TicketTemplate[] | undefined {
  if (typeof v !== "string" || !v) return undefined;
  try {
    const parsed = JSON.parse(v) as { templates?: unknown };
    const templates = parsed?.templates;
    if (!Array.isArray(templates)) return [];
    return templates
      .filter((t): t is TicketTemplate => !!t && typeof (t as TicketTemplate).id === "string")
      .map((t) => ({ id: t.id, doc: t.doc, ticket: t.ticket ?? {}, result: t.result }));
  } catch {
    return undefined; // malformed advertisement — treat as "not advertised"
  }
}

function asBool(v: unknown): boolean | undefined {
  if (typeof v === "boolean") return v;
  if (typeof v === "bigint") return v !== 0n;
  if (typeof v === "number") return v !== 0;
  return undefined;
}

function asStr(v: unknown): string | undefined {
  return typeof v === "string" ? v : undefined;
}

function asNum(v: unknown): number | undefined {
  if (typeof v === "number") return v;
  if (typeof v === "bigint") return Number(v);
  return undefined;
}

export function decodeSqlInfo(table: Table): Capabilities {
  const raw = new Map<number, unknown>();
  const names = table.getChild("info_name");
  const values = table.getChild("value");
  if (names && values) {
    for (let i = 0; i < table.numRows; i++) {
      const code = Number(names.get(i));
      let v: unknown = values.get(i);
      if (typeof v === "bigint" && v >= -9007199254740992n && v <= 9007199254740992n) {
        // small int64 info values (transaction level etc.) read better as numbers
        v = Number(v);
      }
      raw.set(code, v);
    }
  }
  return {
    vendorName: asStr(raw.get(SERVER_NAME)),
    vendorVersion: asStr(raw.get(SERVER_VERSION)),
    arrowVersion: asStr(raw.get(SERVER_ARROW_VERSION)),
    readOnly: asBool(raw.get(SERVER_READ_ONLY)),
    sql: asBool(raw.get(SERVER_SQL)),
    substrait: asBool(raw.get(SERVER_SUBSTRAIT)),
    transactions: asNum(raw.get(SERVER_TRANSACTION)),
    cancel: asBool(raw.get(SERVER_CANCEL)),
    directTickets: decodeDirectTickets(raw.get(SPARROW_DIRECT_TICKETS)),
    raw,
  };
}

export const EMPTY_CAPABILITIES: Capabilities = { raw: new Map() };
