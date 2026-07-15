// Typed query builder — builds the SELECT exactly like `sparrow query` does.
// Deliberately shallow: columns and WHERE clauses pass through verbatim (they
// may be expressions); multiple where() calls AND-join. Not an expression DSL.
import type { QueryStream } from "./query-stream.js";
import type { QueryOptions } from "./types.js";

export interface QueryRunner {
  query(sql: string, opts?: QueryOptions): QueryStream;
}

export class QueryBuilder {
  #runner: QueryRunner;
  #table: string;
  #cols: string[] = [];
  #wheres: string[] = [];
  #orderBy: string | undefined;
  #desc = false;
  #limit: number | undefined;

  constructor(runner: QueryRunner, table: string) {
    this.#runner = runner;
    this.#table = table;
  }

  /** Columns (or expressions) to select. Omit for `*`. */
  select(...cols: string[]): this {
    this.#cols.push(...cols);
    return this;
  }

  /** A WHERE clause fragment; multiple calls AND-join. */
  where(expr: string): this {
    this.#wheres.push(expr);
    return this;
  }

  orderBy(col: string, dir: "asc" | "desc" = "asc"): this {
    this.#orderBy = col;
    this.#desc = dir === "desc";
    return this;
  }

  limit(n: number): this {
    this.#limit = n;
    return this;
  }

  toSQL(): string {
    const cols = this.#cols.length ? this.#cols.join(", ") : "*";
    let sql = `SELECT ${cols} FROM ${this.#table}`;
    if (this.#wheres.length) {
      sql += ` WHERE ${this.#wheres.map((w) => `(${w})`).join(" AND ")}`;
    }
    if (this.#orderBy) sql += ` ORDER BY ${this.#orderBy}${this.#desc ? " DESC" : ""}`;
    if (this.#limit !== undefined) sql += ` LIMIT ${Math.floor(this.#limit)}`;
    return sql;
  }

  /** Execute — returns the same dual-natured QueryStream as client.query(). */
  query(opts?: QueryOptions): QueryStream {
    return this.#runner.query(this.toSQL(), opts);
  }
}
