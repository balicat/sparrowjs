// Automated browser smoke: esbuild-bundle the library entry, serve it on
// localhost, drive headless Chrome (system channel — no browser download)
// against the live gRPC-web edge. Exit 0/1 for CI.
//
//   node test/browser/run.mjs            → https://sparrowflight.io/flight
//   SPARROW_ORIGIN=... node test/browser/run.mjs
import { build } from "esbuild";
import http from "node:http";
import { chromium } from "playwright-core";

const ORIGIN = process.env.SPARROW_ORIGIN ?? "https://sparrowflight.io";
const ENDPOINT = process.env.SPARROW_ENDPOINT ?? `${ORIGIN}/flight`;

const bundle = await build({
  entryPoints: [new URL("./entry.js", import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, "$1")],
  bundle: true,
  format: "iife",
  target: "es2020",
  write: false,
});
const js = bundle.outputFiles[0].text;

const html = `<!doctype html><meta charset="utf-8"><title>sparrowJS smoke</title>
<script>window.__ENDPOINT = ${JSON.stringify(ENDPOINT)};</script>
<script src="/bundle.js"></script>`;

const server = http.createServer((req, res) => {
  if (req.url === "/bundle.js") {
    res.writeHead(200, { "content-type": "text/javascript" });
    res.end(js);
  } else {
    res.writeHead(200, { "content-type": "text/html" });
    res.end(html);
  }
});
await new Promise((r) => server.listen(0, "127.0.0.1", r));
const port = server.address().port;

let browser;
let failed = false;
try {
  browser = await chromium.launch({ channel: "chrome", headless: true });
  const page = await browser.newPage();
  page.on("console", (m) => {
    if (m.type() === "error") console.error("  [page]", m.text());
  });
  await page.goto(`http://127.0.0.1:${port}/`);
  await page.waitForFunction(() => window.__RESULT !== null, null, { timeout: 90_000 });
  const result = await page.evaluate(() => window.__RESULT);

  console.log(`sparrowJS browser smoke → ${ENDPOINT}`);
  for (const step of result.steps) console.log("  ✓", step.join(" · "));
  if (!result.ok) {
    console.error("  ✖", result.error);
    failed = true;
  } else {
    console.log("  browser smoke PASSED");
  }
} catch (e) {
  console.error("  ✖ runner:", e.message ?? e);
  failed = true;
} finally {
  await browser?.close();
  server.close();
}
process.exit(failed ? 1 : 0);
