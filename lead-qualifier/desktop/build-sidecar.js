#!/usr/bin/env node

const { spawnSync } = require("node:child_process");
const path = require("node:path");

const projectRoot = path.resolve(__dirname, "..");
const env = {
  ...process.env,
  PYINSTALLER_CONFIG_DIR: path.join(projectRoot, ".pyinstaller-cache"),
};

const args = [
  "--noconfirm",
  "--clean",
  "--distpath",
  "dist-sidecar",
  "--workpath",
  "build-sidecar",
  "pyinstaller.spec",
];

const candidates = process.platform === "win32"
  ? ["pyinstaller", path.join(projectRoot, ".venv", "Scripts", "pyinstaller.exe")]
  : ["pyinstaller", path.join(projectRoot, ".venv", "bin", "pyinstaller")];

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
  console.error(`Failed to execute pyinstaller: ${result.error.message}`);
  process.exit(1);
}

process.exit(result.status ?? 1);
