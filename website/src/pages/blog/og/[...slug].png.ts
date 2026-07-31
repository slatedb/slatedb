// Per-post Open Graph image. One static PNG is emitted at build time for every
// blog post at /blog/og/<slug>.png, referenced by the post's og:image meta.
import type { APIRoute } from 'astro';
import { getCollection } from 'astro:content';
import { renderOgImage } from '../../../lib/og-image';

export async function getStaticPaths() {
	const posts = await getCollection('blog');
	return posts.map((post) => ({
		params: { slug: post.id },
		props: { post },
	}));
}

export const GET: APIRoute = async ({ props }) => {
	const { post } = props as { post: Awaited<ReturnType<typeof getCollection<'blog'>>>[number] };
	const png = await renderOgImage({ title: post.data.title });
	return new Response(new Uint8Array(png), {
		headers: {
			'Content-Type': 'image/png',
			'Cache-Control': 'public, max-age=31536000, immutable',
		},
	});
};
