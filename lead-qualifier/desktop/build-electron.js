#!/usr/bin/env node

const { spawnSync } = require("node:child_process");
const path = require("node:path");

const projectRoot = path.resolve(__dirname, "..");
const args = process.argv.slice(2);

if (args.length === 0) {
  console.error("Usage: node ./desktop/build-electron.js <electron-builder args>");
  process.exit(1);
}

const owner = process.env.GH_REPO_OWNER || process.env.HOUND_UPDATE_OWNER;
const repo = process.env.GH_REPO_NAME || process.env.HOUND_UPDATE_REPO;

if (!owner || !repo) {
  console.error(
    "Missing GitHub release config. Set GH_REPO_OWNER and GH_REPO_NAME before packaging."
  );
  process.exit(1);
}

const env = {
  ...process.env,
  GH_REPO_OWNER: owner,
  GH_REPO_NAME: repo,
  HOUND_UPDATE_OWNER: owner,
  HOUND_UPDATE_REPO: repo,
};

const candidates = process.platform === "win32"
  ? ["electron-builder", path.join(projectRoot, "node_modules", ".bin", "electron-builder.cmd")]
  : ["electron-builder", path.join(projectRoot, "node_modules", ".bin", "electron-builder")];

let result = null;
for (const command of candidates) {
  result = spawnSync(command, args, {
    cwd: projectRoot,
    env,
    stdio: "inherit",
  });
  if (!(result.error && result.error.code === "ENOENT")) break;
}

if (result.error) {
  console.error(`Failed to execute electron-builder: ${result.error.message}`);
  process.exit(1);
}

process.exit(result.status ?? 1);
