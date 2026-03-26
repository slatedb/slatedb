import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

async function main() {
  const [packageDir, version] = process.argv.slice(2);
  if (!packageDir || !version) {
    throw new Error("usage: prepare-package.mjs <package-dir> <version>");
  }

  const scriptDir = path.dirname(fileURLToPath(import.meta.url));
  const bindingDir = path.resolve(scriptDir, "..");
  const packageJsonPath = path.join(packageDir, "package.json");
  const metadataPath = path.join(bindingDir, "package.metadata.json");

  const [generatedPackageJsonRaw, metadataRaw] = await Promise.all([
    fs.readFile(packageJsonPath, "utf8"),
    fs.readFile(metadataPath, "utf8"),
  ]);

  const generatedPackageJson = JSON.parse(generatedPackageJsonRaw);
  const metadata = JSON.parse(metadataRaw);
  const preparedPackageJson = {
    ...generatedPackageJson,
    ...metadata,
    version,
  };

  await fs.writeFile(
    packageJsonPath,
    `${JSON.stringify(preparedPackageJson, null, 2)}\n`,
    "utf8",
  );
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
