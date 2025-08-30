// Generate MDX wrappers for top-level RFCs so Starlight has frontmatter.
// - Reads Markdown files via the `src/content/docs/rfcs` symlink.
// - Writes wrappers to `src/content/docs/rfcs/` with frontmatter and rendered content.

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const websiteRoot = path.resolve(__dirname, '..');
const wrappersDir = path.join(websiteRoot, 'src/content/docs/rfcs');
const repoRfcsDir = path.resolve(websiteRoot, '..', 'rfcs');

function yamlEscape(str) {
  return JSON.stringify(str);
}

function extractTitle(markdown) {
  const m = markdown.match(/^\s*#\s+(.+)\s*$/m);
  return m ? m[1].trim() : null;
}

function computeOrder(filename) {
  const m = filename.match(/^(\d{4})-/);
  return m ? Number.parseInt(m[1], 10) : undefined;
}

// Escape HTML character entities and raw HTML so Markdown renders them as text.
function escapeHtmlEntities(str) {
  return str
    // First, escape & but not already-encoded entities like &amp; &lt; &#x27; etc.
    .replace(/&(?!(?:[a-zA-Z]+|#\d+|#x[0-9A-Fa-f]+);)/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

export async function generateRfcWrappers() {
  try {
    await fs.mkdir(wrappersDir, { recursive: true });

    // Discover RFC source files directly from repo root.
    const entries = await fs
      .readdir(repoRfcsDir, { withFileTypes: true })
      .catch(() => []);
    const rfcFiles = entries
      .filter((e) => e.isFile() && e.name.endsWith('.md'))
      .map((e) => e.name)
      .sort();

    // Remove wrappers that no longer have a source.
    const existing = await fs.readdir(wrappersDir).catch(() => []);
    for (const f of existing) {
      if (!f.endsWith('.mdx')) continue;
      const base = f.replace(/\.mdx$/, '.md');
      if (!rfcFiles.includes(base)) {
        await fs.unlink(path.join(wrappersDir, f)).catch(() => {});
      }
    }

    for (const name of rfcFiles) {
      const sourcePath = path.join(repoRfcsDir, name);
      const raw = await fs.readFile(sourcePath, 'utf8');
      const title = extractTitle(raw) || name.replace(/\.md$/, '');
      const order = computeOrder(name);
      const short = title.replace(/^SlateDB\s+/i, '');
      const label = order ? `RFC ${String(order).padStart(4, '0')}: ${short}` : title;

      const frontmatter = [
        '---',
        `title: ${yamlEscape(title)}`,
        'sidebar:',
        order ? `  order: ${order}` : undefined,
        `  label: ${yamlEscape(label)}`,
        `editUrl: ${yamlEscape(`https://github.com/slatedb/slatedb/edit/main/rfcs/${name}`)}`,
        '---',
        '',
      ]
        .filter(Boolean)
        .join('\n');

      // Trim the first H1 and escape HTML entities, then write content directly as MDX markdown.
      const contentWithoutH1 = raw.replace(/^\s*#\s+.+?(\r?\n)+/, '');
      const safeContent = escapeHtmlEntities(contentWithoutH1);

      const body = `${frontmatter}\n${safeContent}\n`;

      const outPath = path.join(wrappersDir, name.replace(/\.md$/, '.mdx'));
      const prev = await fs.readFile(outPath, 'utf8').catch(() => null);
      if (prev !== body) {
        await fs.writeFile(outPath, body, 'utf8');
      }
    }
  } catch (err) {
    console.warn('[rfcs-generator] Error:', err?.message || err);
  }
}

// Allow running directly: `node ./scripts/generate-rfcs.js`
if (import.meta.url === pathToFileUrl(process.argv[1]).href) {
  generateRfcWrappers();
}

function pathToFileUrl(p) {
  const u = new URL('file:');
  const abs = path.resolve(p);
  // Ensure proper URL encoding for spaces etc.
  u.pathname = abs.split(path.sep).join('/');
  return u;
}

// No symlink management; wrappers embed content.
