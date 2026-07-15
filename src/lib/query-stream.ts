// QueryStream — one object, two consumption styles:
//   for await (const batch of stream)   → batches as they arrive (README promise)
//   const { table, stats } = await stream → assembled result (demo-factory shape)
// Iterating consumes; awaiting consumes-and-assembles; awaiting after (or
// during) iteration returns the same accumulated result. Iterating twice throws.
//
// Internals (reshaped after the tester's M1 adversarial pass):
// one pull-based inner pump (#run) drives ALL bookkeeping; the public
// iterator, then(), and the schema primer just pull from it. Pull-based
// means awaiting .schema decodes exactly one frame and leaves the rest of
// the stream on the wire (backpressure via generator semantics).
import { RecordBatch, RecordBatchReader, Schema, Table } from "apache-arrow";
import { convertBatch } from "./bigint.js";
import { decorateDecodeError } from "./errors.js";
import type { BigIntMode, QueryOptions, QueryResult, QueryStats } from "./types.js";

/** Timing/size marks recorded by the pump as the wire work happens. */
export class Marks {
  t0 = performance.now();
  authMs = 0;
  planMs = 0;
  /** schema decoded from FlightInfo — heals servers whose empty-result
   *  streams carry a field-less schema (DataFusion/ROAPI, tester F6) */
  fallbackSchema: Schema | undefined;
  tPlanDone = 0;
  tFirstBatch = 0;
  tLastBatch = 0;
  rows = 0;
  batches = 0;
  wireBytes = 0;

  planDone(): void {
    this.tPlanDone = performance.now();
  }
  batch(rows: number): void {
    const now = performance.now();
    if (!this.tFirstBatch) this.tFirstBatch = now;
    this.tLastBatch = now;
    this.rows += rows;
    this.batches++;
  }
  snapshot(): QueryStats {
    const end = this.tLastBatch || performance.now();
    const totalMs = end - this.t0;
    const streamMs = this.tFirstBatch ? this.tLastBatch - this.tFirstBatch : 0;
    const streamWindow = Math.max(1, end - (this.tPlanDone || this.t0));
    const r = (n: number) => Math.round(n * 10) / 10;
    return {
      authMs: r(this.authMs),
      planMs: r(this.planMs),
      firstBatchMs: this.tFirstBatch ? r(this.tFirstBatch - (this.tPlanDone || this.t0)) : 0,
      streamMs: r(streamMs),
      totalMs: r(totalMs),
      rows: this.rows,
      batches: this.batches,
      wireBytes: this.wireBytes,
      rowsPerSec: totalMs > 0 ? Math.round(this.rows / (totalMs / 1000)) : 0,
      mbitPerSec: r((this.wireBytes * 8) / 1000 / streamWindow),
    };
  }
}

/**
 * The pump a client hands to a QueryStream: does the wire work lazily
 * (bootstrap wait, GetFlightInfo, DoGet) and yields one encapsulated-IPC
 * generator PER ENDPOINT. Each endpoint's DoGet is its own IPC stream with
 * its own schema message, so each gets its own reader.
 */
export type EndpointPump = (
  marks: Marks,
  signal: AbortSignal,
) => AsyncGenerator<AsyncGenerator<Uint8Array>>;

function cancelError(): Error {
  const e = new Error("query canceled by cancel()");
  e.name = "AbortError";
  return e;
}

export class QueryStream implements AsyncIterable<RecordBatch>, PromiseLike<QueryResult> {
  readonly #pump: EndpointPump;
  readonly #marks = new Marks();
  readonly #abort = new AbortController();
  readonly #onBatch: QueryOptions["onBatch"];
  readonly #mode: BigIntMode;

  #iterStarted = false;
  #accumulated: RecordBatch[] = [];
  #schemaValue: Schema | undefined;
  #completion: Promise<QueryResult> | undefined;

  // pull machinery: one inner pump, serialized pulls, a replay buffer for
  // frames the schema primer decoded before a consumer arrived
  #inner: AsyncGenerator<RecordBatch> | undefined;
  #pullChain: Promise<unknown> = Promise.resolve();
  #buffered: RecordBatch[] = [];
  #priming = false;

  #resolveSchema!: (s: Schema) => void;
  #rejectSchema!: (e: unknown) => void;
  #schemaSettled = false;
  readonly #schemaPromise: Promise<Schema>;

  #resolveDone!: () => void;
  #rejectDone!: (e: unknown) => void;
  readonly #done: Promise<void>;

  constructor(pump: EndpointPump, opts: QueryOptions = {}, defaultMode: BigIntMode = "bigint") {
    this.#pump = pump;
    this.#onBatch = opts.onBatch;
    this.#mode = opts.bigIntMode ?? defaultMode;
    this.#schemaPromise = new Promise<Schema>((res, rej) => {
      this.#resolveSchema = res;
      this.#rejectSchema = rej;
    });
    this.#done = new Promise<void>((res, rej) => {
      this.#resolveDone = res;
      this.#rejectDone = rej;
    });
    // both settle through controlled paths; avoid unhandled-rejection noise
    // when a consumer only uses one of the two styles
    this.#schemaPromise.catch(() => {});
    this.#done.catch(() => {});
  }

  /**
   * Resolves at the first frame. Accessing it starts the query lazily and
   * decodes exactly ONE frame (kept and replayed to a later iterator) — so a
   * standalone `await stream.schema` settles without consuming the stream.
   */
  get schema(): Promise<Schema> {
    void this.#primeSchema();
    return this.#schemaPromise;
  }

  /** Abort the fetch AND stop batch delivery — in-flight consumption rejects,
   *  even when the transport already buffered the whole (small) response. */
  cancel(): void {
    this.#abort.abort();
  }

  #settleSchema(s: Schema): void {
    if (this.#schemaSettled) return;
    this.#schemaSettled = true;
    this.#schemaValue = s;
    this.#resolveSchema(s);
  }

  // ── the single inner pump: all bookkeeping lives here ───────────────────

  async *#run(): AsyncGenerator<RecordBatch> {
    let failed: unknown;
    try {
      for await (const ipc of this.#pump(this.#marks, this.#abort.signal)) {
        const reader = await RecordBatchReader.from(ipc);
        for await (let batch of reader) {
          // cancel() must bite even when the transport already buffered
          // everything (tester F2) — the signal only stops network pulls
          if (this.#abort.signal.aborted) throw cancelError();
          if (this.#mode !== "bigint") batch = convertBatch(batch, this.#mode);
          // settle AFTER conversion — the assembled Table and the public
          // schema promise must describe the batches the consumer receives
          this.#settleSchema(batch.schema);
          this.#marks.batch(batch.numRows);
          this.#accumulated.push(batch);
          if (this.#onBatch) {
            try {
              this.#onBatch(batch, this.#accumulated.length, Math.round(performance.now() - this.#marks.t0));
            } catch {
              // a rendering hiccup must not kill the stream
            }
          }
          // onBatch may have called cancel() — honor it before yielding more
          if (this.#abort.signal.aborted) throw cancelError();
          yield batch;
        }
        // empty result: the reader still saw the schema message. Some
        // servers (DataFusion/ROAPI) send a FIELD-LESS schema on empty
        // streams — prefer the FlightInfo schema then (tester F6).
        if (reader.schema?.fields.length) this.#settleSchema(reader.schema);
        else if (this.#marks.fallbackSchema) this.#settleSchema(this.#marks.fallbackSchema);
        else if (reader.schema) this.#settleSchema(reader.schema);
      }
    } catch (e) {
      failed = decorateDecodeError(e);
      if (!this.#schemaSettled) {
        this.#schemaSettled = true;
        this.#rejectSchema(failed);
      }
      this.#rejectDone(failed);
      throw failed;
    } finally {
      if (failed === undefined) {
        // normal end OR early break by the consumer — both count as done
        if (!this.#schemaSettled) {
          if (this.#marks.fallbackSchema) {
            this.#settleSchema(this.#marks.fallbackSchema);
          } else {
            this.#schemaSettled = true;
            this.#rejectSchema(new Error("stream ended before a schema arrived"));
          }
        }
        this.#resolveDone();
      }
    }
  }

  /** Serialize inner pulls — the schema primer and an iterator must never
   *  interleave two next() calls on the same generator. */
  #pull(): Promise<IteratorResult<RecordBatch>> {
    this.#inner ??= this.#run();
    const it = this.#inner;
    const p = this.#pullChain.then(() => it.next());
    this.#pullChain = p.catch(() => {});
    return p;
  }

  async #primeSchema(): Promise<void> {
    if (this.#schemaSettled || this.#iterStarted || this.#priming) return;
    this.#priming = true;
    try {
      while (!this.#schemaSettled) {
        const n = await this.#pull();
        if (n.done) break;
        this.#buffered.push(n.value);
      }
    } catch {
      // the schema promise was rejected inside #run; nothing more to do
    }
  }

  async *[Symbol.asyncIterator](): AsyncGenerator<RecordBatch> {
    if (this.#iterStarted) throw new Error("QueryStream already consumed — create a new query");
    this.#iterStarted = true;
    this.#inner ??= this.#run();
    try {
      for (;;) {
        // frames the schema primer already decoded replay first
        if (this.#buffered.length) {
          yield this.#buffered.shift()!;
          continue;
        }
        const n = await this.#pull();
        // a primer pull that was in flight when we started may have landed
        // in the buffer while we awaited — keep wire order by queueing our
        // own pull behind it
        if (this.#buffered.length) {
          if (!n.done) this.#buffered.push(n.value);
          yield this.#buffered.shift()!;
          continue;
        }
        if (n.done) return;
        yield n.value;
      }
    } finally {
      // early break/throw by the consumer: close the pump (resolves #done,
      // releases the wire). No-op after normal completion.
      await this.#inner.return(undefined).then(
        () => undefined,
        () => undefined,
      );
    }
  }

  #result(): Promise<QueryResult> {
    this.#completion ??= (async () => {
      if (!this.#iterStarted) {
        for await (const _ of this) {
          void _; // self-drain; batches accumulate as a side effect
        }
      } else {
        await this.#done;
      }
      const schema = this.#schemaValue ?? (await this.#schemaPromise);
      const table = new Table(schema, this.#accumulated);
      return { table, stats: this.#marks.snapshot() };
    })();
    return this.#completion;
  }

  then<T1 = QueryResult, T2 = never>(
    onfulfilled?: ((value: QueryResult) => T1 | PromiseLike<T1>) | null,
    onrejected?: ((reason: unknown) => T2 | PromiseLike<T2>) | null,
  ): Promise<T1 | T2> {
    return this.#result().then(onfulfilled, onrejected);
  }

  catch<T = never>(onrejected?: ((reason: unknown) => T | PromiseLike<T>) | null): Promise<QueryResult | T> {
    return this.#result().catch(onrejected);
  }

  finally(onfinally?: (() => void) | null): Promise<QueryResult> {
    return this.#result().finally(onfinally);
  }
}
