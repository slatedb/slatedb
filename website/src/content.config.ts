import { defineCollection, z } from 'astro:content';
import { docsLoader } from '@astrojs/starlight/loaders';
import { docsSchema } from '@astrojs/starlight/schema';
import { glob } from 'astro/loaders';

export const collections = {
	docs: defineCollection({ loader: docsLoader(), schema: docsSchema() }),
	blog: defineCollection({
		// Plain MDX/Markdown posts living outside the Starlight docs tree so they
		// render through our own bespoke layout instead of the docs chrome.
		loader: glob({ pattern: '**/*.{md,mdx}', base: './src/content/blog' }),
		schema: z.object({
			title: z.string(),
			pubDate: z.coerce.date(),
			author: z.string(),
			// Optional GitHub handle (no leading @); renders the byline as a link.
			authorGithub: z.string().optional(),
			// Optional per-post social image; falls back to the site default.
			ogImage: z.string().optional(),
		}),
	}),
};
