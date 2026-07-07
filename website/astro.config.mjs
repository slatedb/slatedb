// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightLinksValidator from 'starlight-links-validator';
import starlightLlmsTxt from 'starlight-llms-txt';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { generateRfcWrappers } from './scripts/generate-rfcs.js';
import { generateMdMirrors } from './scripts/generate-md-mirrors.js';
import mermaid from 'astro-mermaid';
import remarkDirective from 'remark-directive';
import remarkAdmonitions from './src/plugins/remark-admonitions.mjs';

const site = 'https://slatedb.io';
const ogUrl = new URL('/img/slatedb-opengraph.jpg', site).href;
const ogImageAlt = 'SlateDB - An embedded database built on object storage';

// Project summary for llms.txt. Kept aligned with the framing in
// public/index.md and the "Introducing SlateDB" blog post so agents that
// cross-check sources see one consistent description.
const projectDescription =
	'SlateDB is an open-source (Apache-2.0) embedded key-value database, implemented as an LSM tree, that depends on object storage alone for durability (Amazon S3, Google Cloud Storage, Azure Blob Storage, MinIO, and Cloudflare R2). Built in async Rust with bindings for Rust, Go, Java, Node, and Python, it is the object-native successor to RocksDB — bringing object storage\'s economics, ~11 nines of durability, and managed replication to online, low-latency workloads.';

// https://astro.build/config
export default defineConfig({
	site,
	// Applies to plain Astro markdown/MDX (e.g. the blog). Starlight docs use
	// Expressive Code instead, so this only affects our bespoke pages.
	markdown: {
		shikiConfig: {
			theme: 'github-light',
			// `ascii-art` isn't a real grammar; treat it as plain text so Shiki
			// doesn't error. Astro keeps `data-language="ascii-art"` on the
			// rendered <pre>, which the blog styles as tightly-spaced ASCII art
			// (box-drawing characters that connect vertically).
			langAlias: { 'ascii-art': 'text' },
		},
		// `:::note` admonitions for the blog. remark-directive parses the syntax;
		// remarkAdmonitions transforms it (and self-scopes to blog files so it
		// doesn't touch Starlight docs' own native asides).
		remarkPlugins: [remarkDirective, remarkAdmonitions],
	},
	integrations: [
		mermaid(),
		starlight({
			title: 'SlateDB',
			description: 'An embedded database built on object storage',
			// Use our own src/pages/404.astro (Sisyphus art) instead of
			// Starlight's bare docs-themed 404, which otherwise wins at
			// the literal /404 URL via injectRoute.
			disable404Route: true,
			logo: {
				src: './public/img/logo-full.svg',
				alt: 'SlateDB',
				replacesTitle: true,
			},
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/slatedb/slatedb' },
				{ icon: 'discord', label: 'Discord', href: 'https://discord.gg/mHYmGy5MgA' },
			],
			components: {
				// Override the default header to remove theme and i18n selectors.
				Header: './src/components/Header.astro',
				// Override the default social icons to configure size and color behavior.
				SocialIcons: './src/components/SocialIcons.astro',
				// Override the default page frame to add the footer.
				PageFrame: './src/components/PageFrame.astro',
				// Override the default theme provider to ensure light mode is always enabled.
				ThemeProvider: './src/components/ThemeProvider.astro',
				Hero: './src/components/Hero.astro',
				// Override the page title to render the frontmatter description as a lede subtitle.
				PageTitle: './src/components/PageTitle.astro',
				// Override the right-hand page sidebar to add a "Copy as Markdown" link below the TOC.
				PageSidebar: './src/components/PageSidebar.astro',
				// Override the sidebar to pin a community-stats card at the bottom.
				Sidebar: './src/components/Sidebar.astro',
				// Extend <Head> to advertise the per-page markdown mirror via <link rel="alternate">.
				Head: './src/components/Head.astro',
			},
			expressiveCode: {
				themes: ['github-light'],
				styleOverrides: {
					borderRadius: '0.75rem',
					borderColor: 'transparent',
					codeFontSize: '0.85rem',
					codeLineHeight: '1.55',
					frames: {
						shadowColor: 'rgba(15, 23, 42, 0.04)',
						editorActiveTabIndicatorTopColor: 'transparent',
						editorActiveTabBorderColor: 'transparent',
						editorTabBarBorderBottomColor: 'transparent',
					},
				},
			},
			customCss: ['./src/styles/custom.css'],
			editLink: {
				baseUrl: 'https://github.com/slatedb/slatedb/edit/main/website/',
			},
			head: [
				{
					tag: 'meta',
					attrs: { property: 'og:image', content: ogUrl },
				},
				{
					tag: 'meta',
					attrs: { property: 'og:image:alt', content: ogImageAlt },
				},
				{
					tag: 'link',
					attrs: { rel: 'preconnect', href: 'https://fonts.googleapis.com' },
				},
				{
					tag: 'link',
					attrs: { rel: 'preconnect', href: 'https://fonts.gstatic.com', crossorigin: '' },
				},
				{
					tag: 'link',
					attrs: {
						rel: 'stylesheet',
						href: 'https://fonts.googleapis.com/css2?family=Marcellus&family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap',
					},
				},
			],
			plugins: [
				// The blog lives in custom (non-Starlight) Astro pages, which the
				// link validator can't resolve — exclude them so links from docs
				// or RFCs into /blog/* aren't reported as invalid.
				starlightLinksValidator({ exclude: ['/blog', '/blog/', '/blog/**'] }),
				starlightLlmsTxt({
				projectName: 'SlateDB',
				description: projectDescription,
				details: [
					'## When to use SlateDB',
					'',
					'SlateDB is a library, not a standalone server or a hosted service. It ships as an embedded engine with no HTTP server; you link it into your own Rust, Go, Java, Node, or Python application and it communicates directly with the object store you configure. There is no network API, no cluster to run, and no local disk of record (a local disk is optional and used only as cache).',
					'',
					'Use it as:',
					'',
					'- The storage core inside any data system (database, cache, stream processor, or workflow engine) you are building.',
					'- A cheap, elastic, object-native replacement for local-disk LSM engines like RocksDB, WiredTiger, or Pebble.',
					'- Durable state for stateless or serverless compute where attaching and replicating local disks is impractical.',
					'- Single-writer, multi-reader deployments that scale reads independently of writes over a shared bucket.',
					'',
					'## Key features',
					'',
					'- Object-store native: durability comes from object storage alone; no disk of record and no replication for you to manage.',
					'- Transactions and snapshot isolation.',
					'- Single-writer, multi-reader deployments.',
					'- Checkpoints and forks: O(1) branching by marking a manifest as retained.',
					'- Rescaling via views: split a database by key range as an O(1) manifest view instead of copying data.',
					'- Pluggable, distributed compaction that can run on separate machines.',
					'',
					'## Trade-offs',
					'',
					'- Object storage request latencies are an order of magnitude higher than local systems (~50-100ms per request); SlateDB batches writes and caches reads to amortize this, but it does not match a local NVMe engine on raw latency.',
					'- It is a key-value / LSM engine, not a SQL database — there is no query planner, no SQL, and no relational schema.',
				].join('\n'),
				optionalLinks: [
					{
						label: 'GitHub repository',
						url: 'https://github.com/slatedb/slatedb',
						description: 'Source code, issues, and releases',
					},
					{
						label: 'Rust API docs (docs.rs)',
						url: 'https://docs.rs/slatedb',
						description: 'Generated API reference for the Rust crate',
					},
					{
						label: 'Discord community',
						url: 'https://discord.gg/mHYmGy5MgA',
						description: 'Questions, discussion, and support',
					},
				],
			}),
			],
			sidebar: [
				{
					label: 'Getting Started',
					items: [
						{
							label: 'Introduction',
							link: '/docs/get-started/introduction/',
						},
						{
							label: 'Quick Start',
							link: '/docs/get-started/quickstart/',
						},
						{
							label: 'FAQ',
							link: '/docs/get-started/faq/',
						}
					]
				},
				{
					label: 'Design',
					collapsed: true,
					items: [
						{
							label: 'Overview',
							link: '/docs/design/overview/',
						},
						{
							label: 'Files',
							link: '/docs/design/files/',
						},
						{
							label: 'SSTs',
							link: '/docs/design/sorted-string-table/',
						},
						{
							label: 'Writes',
							link: '/docs/design/writes/',
						},
						{
							label: 'Reads',
							link: '/docs/design/reads/',
						},
						{
							label: 'Readers',
							link: '/docs/design/readers/',
						},
						{
							label: 'Compaction',
							link: '/docs/design/compaction/',
						},
						{
							label: 'Segmented Compaction',
							link: '/docs/design/segmented-compaction/',
						},
						{
							label: 'Merge Operators',
							link: '/docs/design/merge-operators/',
						},
						{
							label: 'Garbage Collection',
							link: '/docs/design/gc/',
						},
						{
							label: 'Change Data Capture',
							link: '/docs/design/change-data-capture/',
						},
						{
							label: 'Caching',
							link: '/docs/design/caching/',
						},
						{
							label: 'Checkpoints',
							link: '/docs/design/checkpoints/',
						},
						{
							label: 'Compression',
							link: '/docs/design/compression/',
						},
						{
							label: 'Block Transforms',
							link: '/docs/design/block-transformer/',
						},
						{
							label: 'Clones',
							link: '/docs/design/clones/',
						},
						{
							label: 'Time',
							link: '/docs/design/time/',
						},
						{
							label: 'Consistency',
							link: '/docs/design/consistency/',
						}
					]
				},
				{
					label: 'Operations',
					collapsed: true,
					items: [
						{
							label: 'CLI',
							link: '/docs/operations/cli/',
						},
						{
							label: 'Compatibility',
							link: '/docs/operations/compatibility/',
						},
						{
							label: 'Configuration',
							link: '/docs/operations/configuration/',
						},
						{
							label: 'Configuring Foyer Cache',
							link: '/docs/operations/foyer-cache/',
						},
						{
							label: 'Data Modeling',
							link: '/docs/operations/data-modeling/',
						},
						{
							label: 'Errors',
							link: '/docs/operations/errors/',
						},
						{
							label: 'Logging',
							link: '/docs/operations/logging/',
						},
						{
							label: 'Metrics',
							link: '/docs/operations/metrics/',
						},
						{
							label: 'Tuning',
							link: '/docs/operations/tuning/',
						},
						{
							label: 'Benchmarks',
							link: '/docs/operations/benchmarks/',
						}
					]
				},
				{
					label: 'Tutorials',
					collapsed: true,
					items: [
						{
							label: 'Connect to Azure Blob Storage',
							link: '/docs/tutorials/abs/',
						},
						{
							label: 'Connect to S3',
							link: '/docs/tutorials/s3/',
						},
						{
							label: 'Connect to Google Cloud Storage',
							link: '/docs/tutorials/gcs/',
						},
						{
							label: 'Range Scans',
							link: '/docs/tutorials/range-scans/',
						},
						{
							label: 'Checkpoint & Restore',
							link: '/docs/tutorials/checkpoint/',
						},
						{
							label: 'Standalone Compactor',
							link: '/docs/tutorials/standalone-compactor/',
						},
						{
							label: 'Standalone Garbage Collector',
							link: '/docs/tutorials/standalone-garbage-collector/',
						}
					]
				},
				{
					label: 'API Reference',
					collapsed: true,
					items: [
						{
							label: 'Go',
							link: 'https://pkg.go.dev/slatedb.io/slatedb-go/uniffi',
							attrs: { target: '_blank' }
						},
						{
							label: 'Java',
							link: 'https://javadoc.io/doc/io.slatedb/slatedb-uniffi',
							attrs: { target: '_blank' }
						},
						{
							label: 'Node',
							link: 'https://www.jsdocs.io/package/@slatedb/uniffi',
							attrs: { target: '_blank' }
						},
						{
							label: 'Python',
							link: 'https://slatedb.readthedocs.io/',
							attrs: { target: '_blank' }
						},
						{
							label: 'Rust',
							link: 'https://docs.rs/slatedb',
							attrs: { target: '_blank' }
						}
					]
				},
				{
					label: 'RFCs',
					collapsed: true,
					autogenerate: { directory: 'rfcs' },
				},
			]
		}),
	],
	vite: {
		assetsInclude: ['**/*.riv'],
		plugins: [
			{
				name: 'slatedb-rfcs-generator',
				async buildStart() {
					await generateRfcWrappers();
				},
				configureServer(server) {
					// Initial generation at dev server start.
					generateRfcWrappers();
					// Watch rfcs in the repo.
					const repoRfcsGlob = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', 'rfcs/**/*.md');
					server.watcher.add(repoRfcsGlob);
					/** @param {string} file */
					const onChange = (file) => {
						if (file.endsWith('.md') && file.includes(`${path.sep}rfcs${path.sep}`)) {
							generateRfcWrappers();
						}
					};
					server.watcher.on('add', onChange);
					server.watcher.on('change', onChange);
					server.watcher.on('unlink', onChange);
				},
			},
			{
				name: 'slatedb-md-mirrors',
				// Run after the RFC wrappers exist, so we can mirror them too.
				async buildStart() {
					await generateRfcWrappers();
					await generateMdMirrors();
				},
				configureServer(server) {
					generateMdMirrors();
					/** @param {string} file */
					const onChange = (file) => {
						const isDocSource =
							(file.endsWith('.mdx') || file.endsWith('.md')) &&
							(file.includes(`${path.sep}src${path.sep}content${path.sep}docs${path.sep}`));
						if (isDocSource) {
							generateMdMirrors();
						}
					};
					server.watcher.on('add', onChange);
					server.watcher.on('change', onChange);
					server.watcher.on('unlink', onChange);
				},
			},
		],
		server: {
			// No special FS allowances needed; wrappers embed content.
		},
	},
});
