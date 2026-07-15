import { test } from "node:test";
import assert from "node:assert/strict";
import { QueryBuilder } from "../../dist/lib/builder.js";

const runner = { query: (sql) => sql }; // toSQL tests never execute

test("select * by default", () => {
  assert.equal(new QueryBuilder(runner, "t").toSQL(), "SELECT * FROM t");
});

test("cols, where, order, limit — the sparrow query shape", () => {
  const sql = new QueryBuilder(runner, "series_data")
    .select("period", "value")
    .where("series_id = 'PET.RWTC.D'")
    .orderBy("period", "desc")
    .limit(1000)
    .toSQL();
  assert.equal(
    sql,
    "SELECT period, value FROM series_data WHERE (series_id = 'PET.RWTC.D') ORDER BY period DESC LIMIT 1000",
  );
});

test("multiple where() AND-join with parens", () => {
  const sql = new QueryBuilder(runner, "t").where("a = 1").where("b > 2 OR c < 3").toSQL();
  assert.equal(sql, "SELECT * FROM t WHERE (a = 1) AND (b > 2 OR c < 3)");
});

test("orderBy defaults asc (no DESC suffix)", () => {
  assert.equal(new QueryBuilder(runner, "t").orderBy("x").toSQL(), "SELECT * FROM t ORDER BY x");
});

test("limit floors non-integers", () => {
  assert.equal(new QueryBuilder(runner, "t").limit(10.9).toSQL(), "SELECT * FROM t LIMIT 10");
});

test("query() hands toSQL() to the runner", () => {
  assert.equal(new QueryBuilder(runner, "t").limit(1).query(), "SELECT * FROM t LIMIT 1");
});
