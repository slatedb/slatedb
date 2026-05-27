// Generate per-page .md mirrors of every docs page so LLM agents (and humans
// who want raw markdown) can fetch /docs/<slug>.md alongside the HTML route.
//
// Output lands in public/<slug>.md so Astro copies it verbatim into dist/.
// e.g. src/content/docs/docs/get-started/quickstart.mdx
//        -> dist/docs/get-started/quickstart.md
//      src/content/docs/rfcs/0001-foo.md
//        -> dist/docs/rfcs/0001-foo.md
//
// v1 transform is intentionally simple (regex-based, lossy). Tab groups
// collapse to sequential headed sections. Imported `?raw` code blocks are
// inlined from disk. Unknown JSX tags are stripped. Real AST work deferred.

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const websiteRoot = path.resolve(__dirname, '..');
const docsRoot = path.join(websiteRoot, 'src/content/docs/docs');
const rfcsRoot = path.join(websiteRoot, 'src/content/docs/rfcs');
const outRoot = path.join(websiteRoot, 'public/docs');
const repoRoot = path.resolve(websiteRoot, '..');

async function walk(dir) {
  const out = [];
  let entries;
  try {
    entries = await fs.readdir(dir, { withFileTypes: true });
  } catch {
    return out;
  }
  for (const e of entries) {
    const full = path.join(dir, e.name);
    if (e.isDirectory()) {
      out.push(...(await walk(full)));
    } else if (e.isFile() && (e.name.endsWith('.mdx') || e.name.endsWith('.md'))) {
      out.push(full);
    }
  }
  return out;
}

function parseFrontmatter(raw) {
  const m = raw.match(/^---\n([\s\S]*?)\n---\n?/);
  if (!m) return { data: {}, body: raw };
  const block = m[1];
  const body = raw.slice(m[0].length);
  const data = {};
  for (const line of block.split('\n')) {
    const kv = line.match(/^([A-Za-z0-9_-]+):\s*(.*)$/);
    if (kv) {
      let v = kv[2].trim();
      if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
        v = v.slice(1, -1);
      }
      data[kv[1]] = v;
    }
  }
  return { data, body };
}

// Track resolved ?raw imports so we can inline <Code code={ident} ... /> uses.
function collectRawImports(body) {
  const imports = {};
  const re = /import\s+(\w+)\s+from\s+['"]([^'"]+)['"];?/g;
  let m;
  while ((m = re.exec(body))) {
    const ident = m[1];
    const spec = m[2];
    if (spec.endsWith('?raw')) {
      imports[ident] = spec.replace(/\?raw$/, '');
    }
  }
  return imports;
}

function stripImportLines(body) {
  return body.replace(/^\s*import\s+[\s\S]*?from\s+['"][^'"]+['"];?\s*\n/gm, '');
}

// Inline <Code code={ident} lang="..." title="..." /> references by reading
// the file the matching `import ident from '...?raw'` referenced.
async function inlineCodeComponents(body, rawImports, sourceFilePath) {
  const re = /<Code\s+([^/>]*?)\/>/g;
  const replacements = [];
  let m;
  while ((m = re.exec(body))) {
    const attrs = m[1];
    const codeIdent = attrs.match(/code=\{(\w+)\}/)?.[1];
    const lang = attrs.match(/lang=["']([^"']+)["']/)?.[1] || '';
    const title = attrs.match(/title=["']([^"']+)["']/)?.[1] || '';
    let inlined = '';
    if (codeIdent && rawImports[codeIdent]) {
      let target = rawImports[codeIdent];
      // Resolve relative to the source MDX, or to repo root for leading-slash specs.
      if (target.startsWith('/')) {
        target = path.join(repoRoot, target.replace(/^\//, ''));
      } else {
        target = path.resolve(path.dirname(sourceFilePath), target);
      }
      try {
        const fileContents = await fs.readFile(target, 'utf8');
        const titleLine = title ? `// ${title}\n` : '';
        inlined = '```' + lang + '\n' + titleLine + fileContents.trimEnd() + '\n```';
      } catch {
        inlined = `<!-- could not inline ${target} -->`;
      }
    }
    replacements.push({ start: m.index, end: m.index + m[0].length, inlined });
  }
  // Apply in reverse so indices stay valid.
  let result = body;
  for (let i = replacements.length - 1; i >= 0; i--) {
    const r = replacements[i];
    result = result.slice(0, r.start) + r.inlined + result.slice(r.end);
  }
  return result;
}

// Strip the minimum common leading whitespace from a block of text.
// MDX tab content is typically indented inside <TabItem>; that indent
// breaks fenced code blocks if not removed.
function dedent(text) {
  const lines = text.split('\n');
  let min = Infinity;
  for (const line of lines) {
    if (line.trim() === '') continue;
    const m = line.match(/^[ \t]*/);
    if (m) min = Math.min(min, m[0].length);
  }
  if (!isFinite(min) || min === 0) return text;
  return lines.map((l) => l.slice(min)).join('\n');
}

// Flatten <Tabs ...> blocks: each <TabItem label="X"> becomes a "### X" section.
function flattenTabs(body) {
  return body.replace(
    /<Tabs[^>]*>([\s\S]*?)<\/Tabs>/g,
    (_match, inner) => {
      const items = [];
      const itemRe = /<TabItem\s+([^>]*)>([\s\S]*?)<\/TabItem>/g;
      let mm;
      while ((mm = itemRe.exec(inner))) {
        const attrs = mm[1];
        const label = attrs.match(/label=["']([^"']+)["']/)?.[1] || 'Tab';
        const content = dedent(mm[2]).trim();
        items.push(`### ${label}\n\n${content}`);
      }
      return items.join('\n\n');
    },
  );
}

// Strip remaining unknown JSX tags (open/close/self-closing).
function stripRemainingJsx(body) {
  return body
    // Self-closing tags like <Foo bar="x" />
    .replace(/<[A-Z][A-Za-z0-9]*\s*[^/>]*\/>/g, '')
    // Paired tags like <Foo>...</Foo> — keep inner content
    .replace(/<\/?[A-Z][A-Za-z0-9]*[^>]*>/g, '');
}

function normalizeWhitespace(body) {
  return body.replace(/\n{3,}/g, '\n\n').replace(/[ \t]+\n/g, '\n').trim() + '\n';
}

async function transformMdx(sourcePath) {
  const raw = await fs.readFile(sourcePath, 'utf8');
  const { data, body } = parseFrontmatter(raw);

  const rawImports = collectRawImports(body);
  let out = body;
  out = stripImportLines(out);
  out = await inlineCodeComponents(out, rawImports, sourcePath);
  out = flattenTabs(out);
  out = stripRemainingJsx(out);

  // Promote frontmatter title + description to an H1 + lede paragraph.
  const header = [];
  if (data.title) header.push(`# ${data.title}`, '');
  if (data.description) header.push(`> ${data.description}`, '');

  return normalizeWhitespace(header.join('\n') + '\n' + out);
}

// Compute the public URL slug for a content file (mirrors Starlight routing).
function slugFor(sourcePath) {
  const inDocs = sourcePath.startsWith(docsRoot);
  const inRfcs = sourcePath.startsWith(rfcsRoot);
  if (!inDocs && !inRfcs) return null;
  const rel = path
    .relative(inDocs ? docsRoot : rfcsRoot, sourcePath)
    .replace(/\\/g, '/')
    .replace(/\.(mdx?|md)$/, '');
  // RFCs live under /rfcs/ in URLs, not /docs/rfcs/.
  if (inRfcs) return `rfcs/${rel}`;
  return rel;
}

export async function generateMdMirrors() {
  try {
    const files = [...(await walk(docsRoot)), ...(await walk(rfcsRoot))];
    const written = new Set();

    for (const file of files) {
      const slug = slugFor(file);
      if (!slug) continue;
      const md = await transformMdx(file);
      // RFC files don't live under /docs/ in URLs; mirror that.
      const subdir = slug.startsWith('rfcs/') ? '' : 'docs/';
      const outFile = path.join(websiteRoot, 'public', `${subdir}${slug}.md`);
      const finalPath = subdir === ''
        ? path.join(websiteRoot, 'public', `${slug}.md`)
        : outFile;
      await fs.mkdir(path.dirname(finalPath), { recursive: true });
      const prev = await fs.readFile(finalPath, 'utf8').catch(() => null);
      if (prev !== md) {
        await fs.writeFile(finalPath, md, 'utf8');
      }
      written.add(finalPath);
    }

    // Prune stale .md mirrors that no longer have a source. Only walk our two
    // managed output trees so we don't touch unrelated public/ files.
    const managed = [path.join(websiteRoot, 'public/docs'), path.join(websiteRoot, 'public/rfcs')];
    for (const root of managed) {
      const existing = await walk(root);
      for (const f of existing) {
        if (!f.endsWith('.md')) continue;
        if (!written.has(f)) {
          await fs.unlink(f).catch(() => {});
        }
      }
    }
  } catch (err) {
    console.warn('[md-mirrors] Error:', err?.message || err);
  }
}

// Allow running directly: `node ./scripts/generate-md-mirrors.js`
const argv1 = process.argv[1];
if (argv1 && import.meta.url === pathToFileUrl(argv1).href) {
  generateMdMirrors();
}

function pathToFileUrl(p) {
  const u = new URL('file:');
  const abs = path.resolve(p);
  u.pathname = abs.split(path.sep).join('/');
  return u;
}
