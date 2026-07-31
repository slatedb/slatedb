import rss from '@astrojs/rss';
import { getCollection } from 'astro:content';
import type { APIContext } from 'astro';

export async function GET(context: APIContext) {
	const posts = (await getCollection('blog')).sort(
		(a, b) => b.data.pubDate.getTime() - a.data.pubDate.getTime(),
	);

	return rss({
		title: 'SlateDB Blog',
		description: 'News, releases, and engineering notes from the SlateDB project.',
		site: context.site ?? 'https://slatedb.io',
		items: posts.map((post) => ({
			title: post.data.title,
			pubDate: post.data.pubDate,
			author: post.data.author,
			link: `/blog/${post.id}/`,
		})),
	});
}
