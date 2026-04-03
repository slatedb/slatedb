import {
  cpSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  rmSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const PACKAGE_DIR = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(PACKAGE_DIR, "..", "..");
const UNIFFI_MANIFEST_PATH = join(REPO_ROOT, "bindings", "uniffi", "Cargo.toml");
const NAMESPACE = "slatedb";
const CRATE_NAME = "slatedb-uniffi";
const CDYLIB_NAME = "slatedb_uniffi";
const GENERATED_PATHS = [
  "index.js",
  "index.d.ts",
  `${NAMESPACE}.js`,
  `${NAMESPACE}.d.ts`,
  `${NAMESPACE}-ffi.js`,
  `${NAMESPACE}-ffi.d.ts`,
  "runtime",
  "prebuilds",
];
const SUPPORTED_PREBUILD_TARGETS = new Set([
  "linux-x64-gnu",
  "linux-arm64-gnu",
  "darwin-x64",
  "darwin-arm64",
  "win32-x64",
  "win32-arm64",
]);

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: REPO_ROOT,
    stdio: "inherit",
    ...options,
  });

  if (result.status !== 0) {
    throw new Error(`command failed: ${command} ${args.join(" ")}`);
  }
}

function parseArgs(argv) {
  let prebuiltDir = null;

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--prebuilt-dir") {
      prebuiltDir = argv[index + 1];
      index += 1;
      continue;
    }
    if (arg.startsWith("--prebuilt-dir=")) {
      prebuiltDir = arg.slice("--prebuilt-dir=".length);
      continue;
    }
    throw new Error(`unknown argument: ${arg}`);
  }

  if (prebuiltDir == null) {
    return { prebuiltDir: null };
  }

  const resolvedPrebuiltDir = resolve(REPO_ROOT, prebuiltDir);
  return { prebuiltDir: resolvedPrebuiltDir };
}

function currentLinuxLibc() {
  return process.report?.getReport?.().header?.glibcVersionRuntime == null
    ? "musl"
    : "gnu";
}

function hostPrebuildTarget() {
  if (process.platform === "darwin") {
    if (process.arch === "x64" || process.arch === "arm64") {
      return `darwin-${process.arch}`;
    }
    throw new Error(`unsupported macOS architecture: ${process.arch}`);
  }

  if (process.platform === "win32") {
    if (process.arch === "x64" || process.arch === "arm64") {
      return `win32-${process.arch}`;
    }
    throw new Error(`unsupported Windows architecture: ${process.arch}`);
  }

  if (process.platform === "linux") {
    if (process.arch !== "x64" && process.arch !== "arm64") {
      throw new Error(`unsupported Linux architecture: ${process.arch}`);
    }

    const libc = currentLinuxLibc();
    if (libc !== "gnu") {
      throw new Error("Linux musl targets are not packaged for the Node binding");
    }
    return `linux-${process.arch}-${libc}`;
  }

  throw new Error(`unsupported host platform: ${process.platform}`);
}

function nativeLibraryFilename(platform = process.platform) {
  if (platform === "win32") {
    return `${CDYLIB_NAME}.dll`;
  }
  if (platform === "darwin") {
    return `lib${CDYLIB_NAME}.dylib`;
  }
  return `lib${CDYLIB_NAME}.so`;
}

function cleanGeneratedOutputs() {
  for (const relativePath of GENERATED_PATHS) {
    rmSync(join(PACKAGE_DIR, relativePath), { force: true, recursive: true });
  }
}

function buildLocalHostLibrary() {
  run("cargo", ["build", "-p", CRATE_NAME]);
  const libraryPath = join(REPO_ROOT, "target", "debug", nativeLibraryFilename());
  if (!existsSync(libraryPath)) {
    throw new Error(`built host library is missing at ${libraryPath}`);
  }
  return libraryPath;
}

function ensureGeneratedBindings(tempOutputDir) {
  const requiredPaths = [
    "index.js",
    "index.d.ts",
    `${NAMESPACE}.js`,
    `${NAMESPACE}.d.ts`,
    `${NAMESPACE}-ffi.js`,
    `${NAMESPACE}-ffi.d.ts`,
    "runtime",
  ];

  for (const relativePath of requiredPaths) {
    if (!existsSync(join(tempOutputDir, relativePath))) {
      throw new Error(`generator output is missing ${relativePath}`);
    }
  }
}

function generateBindings(libraryPath, tempOutputDir) {
  run("uniffi-bindgen-node-js", [
    "generate",
    libraryPath,
    "--manifest-path",
    UNIFFI_MANIFEST_PATH,
    "--crate-name",
    CRATE_NAME,
    "--out-dir",
    tempOutputDir,
  ]);
  ensureGeneratedBindings(tempOutputDir);
}

function copyGeneratedBindings(tempOutputDir) {
  for (const entry of readdirSync(tempOutputDir, { withFileTypes: true })) {
    if (entry.name === "package.json") {
      continue;
    }

    cpSync(join(tempOutputDir, entry.name), join(PACKAGE_DIR, entry.name), {
      force: true,
      recursive: true,
    });
  }
}

function stageLocalPrebuild(hostLibraryPath) {
  const target = hostPrebuildTarget();
  const destinationDir = join(PACKAGE_DIR, "prebuilds", target);
  mkdirSync(destinationDir, { recursive: true });
  cpSync(hostLibraryPath, join(destinationDir, nativeLibraryFilename()), {
    force: true,
    recursive: false,
  });
}

function stageProvidedPrebuilds(prebuiltDir) {
  const hostTarget = hostPrebuildTarget();
  let stagedTargets = 0;

  for (const entry of readdirSync(prebuiltDir, { withFileTypes: true })) {
    if (!entry.isDirectory()) {
      continue;
    }
    if (!SUPPORTED_PREBUILD_TARGETS.has(entry.name)) {
      continue;
    }

    cpSync(join(prebuiltDir, entry.name), join(PACKAGE_DIR, "prebuilds", entry.name), {
      force: true,
      recursive: true,
    });
    stagedTargets += 1;
  }

  if (stagedTargets === 0) {
    throw new Error(`no supported prebuild targets were found in ${prebuiltDir}`);
  }
  if (!existsSync(join(PACKAGE_DIR, "prebuilds", hostTarget))) {
    throw new Error(`prebuilt host target ${hostTarget} is missing from ${prebuiltDir}`);
  }
}

function resolveLibraryPath(prebuiltDir) {
  if (prebuiltDir == null) {
    return buildLocalHostLibrary();
  }

  const hostTarget = hostPrebuildTarget();
  const platform = process.platform;
  const libraryPath = join(prebuiltDir, hostTarget, nativeLibraryFilename(platform));
  if (!existsSync(libraryPath)) {
    throw new Error(`prebuilt host library is missing at ${libraryPath}`);
  }
  return libraryPath;
}

function main() {
  const { prebuiltDir } = parseArgs(process.argv.slice(2));
  const tempOutputDir = mkdtempSync(join(tmpdir(), "slatedb-node-build-"));

  try {
    cleanGeneratedOutputs();
    const libraryPath = resolveLibraryPath(prebuiltDir);
    generateBindings(libraryPath, tempOutputDir);
    copyGeneratedBindings(tempOutputDir);
    if (prebuiltDir == null) {
      stageLocalPrebuild(libraryPath);
    } else {
      stageProvidedPrebuilds(prebuiltDir);
    }
  } finally {
    rmSync(tempOutputDir, { force: true, recursive: true });
  }
}

main();
