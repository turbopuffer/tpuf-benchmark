import { defineConfig, Plugin } from "vite";
import react from "@vitejs/plugin-react";
import { viteSingleFile } from "vite-plugin-singlefile";
import { readFileSync } from "fs";
import { resolve } from "path";

// In dev mode, inject sample fixture data as window.__TPUFBENCH_DATA__
function devFixturePlugin(): Plugin {
  return {
    name: "dev-fixture-inject",
    apply: "serve",
    transformIndexHtml(html) {
      const fixturePath = resolve(__dirname, "src/fixtures/sample-data.json");
      const data = readFileSync(fixturePath, "utf-8");
      return html.replace(
        "</head>",
        `<script>window.__TPUFBENCH_DATA__ = ${data};</script>\n</head>`
      );
    },
  };
}

export default defineConfig({
  plugins: [react(), viteSingleFile(), devFixturePlugin()],
});
