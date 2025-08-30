// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightLinksValidator from 'starlight-links-validator';
import starlightLlmsTxt from 'starlight-llms-txt';

const site = 'https://slatedb.io';
const ogUrl = new URL('/img/slatedb-opengraph.jpg', site).href;
const ogImageAlt = 'SlateDB - An embedded database built on object storage';

// https://astro.build/config
export default defineConfig({
	site,
	integrations: [
		starlight({
			title: 'SlateDB',
			description: 'An embedded database built on object storage',
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
				// Override the default hero to be able to use the Rive animation.
				Hero: './src/components/Hero.astro',
				// Override the default page frame to add the footer.
				PageFrame: './src/components/PageFrame.astro',
				// Override the default theme provider to ensure dark mode is always enabled.
				ThemeProvider: './src/components/ThemeProvider.astro',
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
							link: '/docs/introduction/',
						},
						{
							label: 'Quick Start',
							link: '/docs/quickstart/',
						},
						{
							label: 'FAQ',
							link: '/docs/faq/',
						},
						{
							label: 'Benchmarks',
							link: '/docs/benchmarks/',
						}
					]
				},
				{
					label: 'Design',
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
							label: 'Manifest',
							link: '/docs/design/manifest/',
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
							label: 'Caching',
							link: '/docs/design/caching/',
						},
						{
							label: 'Compaction',
							link: '/docs/design/compaction/',
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
							label: 'Clones',
							link: '/docs/design/clones/',
						},
						{
							label: 'Time',
							link: '/docs/design/time/',
						}
					]
				},
				{
					label: 'Operations',
					items: [
						{
							label: 'Admin CLI',
							link: '/docs/operations/admin-cli/',
						},
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
							label: 'Performance',
							link: '/docs/operations/performance/',
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
							label: 'Checkpoint & Restore',
							link: '/docs/tutorials/checkpoint/',
						}
					]
				},
				{
					label: 'API Reference',
					collapsed: true,
					items: [
						{
							label: 'Go',
							link: 'https://pkg.go.dev/slatedb.io/slatedb-go',
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
					label: 'Community',
					items: [
						{
							label: 'Discord',
							link: 'https://discord.gg/mHYmGy5MgA',
							attrs: { target: '_blank' }
						},
						{
							label: 'Dosu',
							link: 'https://app.dosu.dev/d8f2da6d-6c4e-43a9-b5f2-b03db801b4d1/ask',
							attrs: { target: '_blank' }
						},
						{
							label: 'GitHub',
							link: 'https://github.com/slatedb/slatedb',
							attrs: { target: '_blank' }
						}
					]
				},
			]
		}),
	],
	vite: {
		assetsInclude: ['**/*.riv'],
	},
});
