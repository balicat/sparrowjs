// Post-deploy check: drive the LIVE demo page (sparrowflight.io/demo/js) in
// headless Chrome and run one query through the deployed bundle, same-origin.
//   node test/browser/demo-live.mjs
import { chromium } from "playwright-core";

const URL = process.env.DEMO_URL ?? "https://sparrowflight.io/demo/js";
let browser;
let failed = false;
try {
  browser = await chromium.launch({ channel: "chrome", headless: true });
  const page = await browser.newPage();
  page.on("console", (m) => {
    if (m.type() === "error") console.error("  [page]", m.text());
  });
  await page.goto(URL, { waitUntil: "domcontentloaded" });
  await page.waitForFunction(() => typeof window.SparrowJS?.createSparrowClient === "function", null, {
    timeout: 30_000,
  });
  const r = await page.evaluate(async () => {
    const c = window.SparrowJS.createSparrowClient({
      endpoint: location.origin + "/flight",
      user: "demo",
      pass: "",
    });
    let batches = 0;
    const res = await c.query("SELECT series_id, period, value FROM series_data LIMIT 5000", {
      onBatch: () => batches++,
    });
    return {
      rows: res.rows,
      batches: res.batches,
      onBatch: batches,
      bytes: res.bytes,
      timing: res.timing,
      cols: res.cols,
    };
  });
  console.log(`live demo bundle → ${URL}`);
  console.log(
    `  ✓ ${r.rows} rows · ${r.batches} batches (onBatch ${r.onBatch}) · ${r.bytes} B · plan ${r.timing.plan} ms · total ${r.timing.total} ms`,
  );
  if (r.rows !== 5000 || r.onBatch < 1 || !r.timing.total) {
    console.error("  ✖ shape mismatch");
    failed = true;
  } else {
    console.log("  live demo PASSED");
  }
} catch (e) {
  console.error("  ✖", e.message ?? e);
  failed = true;
} finally {
  await browser?.close();
}
process.exit(failed ? 1 : 0);
