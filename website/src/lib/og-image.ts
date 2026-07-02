// Build-time Open Graph card renderer for blog posts.
//
// The card is a light, blueprint-style composition: a wide pencil drawing of
// Doric column ruins flanking the sides, a clear parchment center for the
// Marcellus title, and the `slatedb` wordmark along the bottom. satori renders
// the text/logo layer to SVG; sharp composites it over the artwork.
// Used by src/pages/blog/og/[...slug].png.ts — one image per post.

import fs from 'node:fs/promises';
import path from 'node:path';
import satori from 'satori';
import sharp from 'sharp';

// Resolve from the project root: this module is bundled into dist/ at build
// time, so import.meta.url is unreliable, but cwd is always the website root
// during `astro build` (and `astro dev`).
const assetsDir = path.resolve(process.cwd(), 'src/assets');
const fontsDir = path.join(assetsDir, 'og-fonts');
const publicDir = path.resolve(process.cwd(), 'public');

// Brand tokens (mirrored from the blog layout's light surface).
const INK = '#0f1620'; // --ink-1, title
const PARCHMENT = '#efeee9'; // matches the drawing's paper

const WIDTH = 1200;
const HEIGHT = 630;

// Load once and reuse across every post in a single build.
let cache: {
	marcellus: Buffer;
	wordmark: string; // dark wordmark as a data URI
	background: Buffer; // column artwork, pre-cropped to the card
} | null = null;

async function load() {
	if (cache) return cache;
	const [marcellus, logoRaw, background] = await Promise.all([
		fs.readFile(path.join(fontsDir, 'Marcellus-Regular.ttf')),
		fs.readFile(path.join(publicDir, 'img/logo-full.svg'), 'utf8'),
		// Cover-crop the wide column drawing to the card, seat any transparency on
		// parchment, then wash it back with a translucent parchment veil so the
		// columns read as a faint backdrop the title can sit over.
		sharp(path.join(assetsDir, 'og/doric-wide.png'))
			.resize(WIDTH, HEIGHT, { fit: 'cover', position: 'centre' })
			.flatten({ background: PARCHMENT })
			.composite([
				{
					input: {
						create: {
							width: WIDTH,
							height: HEIGHT,
							channels: 4,
							background: { r: 0xef, g: 0xee, b: 0xe9, alpha: 0.55 },
						},
					},
				},
			])
			.toBuffer(),
	]);
	// The source wordmark already fills black — perfect for the light card.
	const wordmark = `data:image/svg+xml;base64,${Buffer.from(logoRaw).toString('base64')}`;
	cache = { marcellus, wordmark, background };
	return cache;
}

// Longer titles step down in size so they stay on a few lines within the clear
// center band without colliding with the columns.
function titleSize(title: string): number {
	const n = title.length;
	if (n <= 28) return 70;
	if (n <= 48) return 58;
	if (n <= 70) return 48;
	return 42;
}

interface OgInput {
	title: string;
}

export async function renderOgImage({ title }: OgInput): Promise<Buffer> {
	const { marcellus, wordmark, background } = await load();

	const tree = {
		type: 'div',
		props: {
			style: {
				position: 'relative',
				width: '100%',
				height: '100%',
				display: 'flex',
				flexDirection: 'column',
				justifyContent: 'space-between',
				alignItems: 'flex-start',
				padding: '64px 80px',
				// Dark hairline frame so the light card keeps a defined edge against a
				// white (light-mode) timeline. Inset so X's rounded corner crop can't
				// clip it.
				boxShadow: 'inset 0 0 0 2px rgba(15,22,32,0.14)',
			},
			children: [
				// Title, left-aligned and vertically centered, sitting directly over
				// the (washed-back) artwork with no backing panel.
				{
					type: 'div',
					props: {
						style: { flex: 1, display: 'flex', alignItems: 'center' },
						children: {
							type: 'div',
							props: {
								style: {
									display: 'flex',
									fontFamily: 'Marcellus',
									fontSize: titleSize(title),
									lineHeight: 1.14,
									color: INK,
									letterSpacing: '-0.01em',
									maxWidth: 840,
								},
								children: title,
							},
						},
					},
				},
				// Wordmark, bottom-left (shares the title's left edge).
				{
					type: 'img',
					props: { src: wordmark, height: 38, style: { height: 38 } },
				},
			],
		},
	};

	const svg = await satori(tree as Parameters<typeof satori>[0], {
		width: WIDTH,
		height: HEIGHT,
		fonts: [{ name: 'Marcellus', data: marcellus, weight: 400, style: 'normal' }],
	});

	// Composite the text/logo layer over the column artwork.
	return sharp(background)
		.composite([{ input: Buffer.from(svg), top: 0, left: 0 }])
		.png()
		.toBuffer();
}
