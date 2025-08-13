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
					label: 'Start Here',
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
							label: 'Frequently Asked Questions',
							link: '/docs/faq/',
						}
					]
				},
				{
					label: 'Reference',
					items: [
						{
							label: 'Architecture',
							link: '/docs/architecture/',
						},
						{
							label: 'Performance',
							link: '/docs/performance/',
						},
						{
							label: 'Logging',
							link: '/docs/operations/logging/',
						},
						{
							label: 'Checkpoint & Restore',
							link: '/docs/tutorials/checkpoint/',
						},
						{
							label: 'API Documentation',
							link: 'https://docs.rs/slatedb',
							attrs: { target: '_blank' }
						}
					]
				},
				{
					label: 'Tutorials',
					items: [
						{
							label: 'Connect to Azure Blob Storage',
							link: '/docs/tutorials/abs/',
						},
						{
							label: 'Connect to S3',
							link: '/docs/tutorials/s3/',
						}
					]
				}
			]
		}),
	],
	vite: {
		assetsInclude: ['**/*.riv'],
	},
});
