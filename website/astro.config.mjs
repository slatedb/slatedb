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

const site = 'https://slatedb.io';
const ogUrl = new URL('/img/slatedb-opengraph.jpg', site).href;
const ogImageAlt = 'SlateDB - An embedded database built on object storage';

// https://astro.build/config
export default defineConfig({
	site,
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
				starlightLinksValidator(),
				starlightLlmsTxt(),
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
							label: 'Range Reads',
							link: '/docs/tutorials/range-reads/',
						},
						{
							label: 'Checkpoint & Restore',
							link: '/docs/tutorials/checkpoint/',
						},
						{
							label: 'Standalone Compactor',
							link: '/docs/tutorials/standalone-compactor/',
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
