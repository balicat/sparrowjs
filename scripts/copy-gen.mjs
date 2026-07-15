// tsc emits src/lib + src/index; the generated proto stubs (plain ESM .js
// with hand-shipped .d.ts) are copied verbatim so dist is self-contained.
import { cpSync, mkdirSync } from "node:fs";
mkdirSync("dist/gen", { recursive: true });
cpSync("src/gen", "dist/gen", { recursive: true });
console.log("copied src/gen -> dist/gen");
