# Ralph Wiggum Loop

You are in a Ralph Wiggum loop. This `PROMPT.md` file is fed back to you on each iteration. Treat it as persistent instructions plus task state.

Your goal is to write accurate SlateDB documentation for the empty pages listed in the `## TODO` section below.

Rules:
- Use the repository code and existing docs as the source of truth.
- Match the tone, structure, and level of detail used in nearby files under `website/src/content/docs/docs/`.
- Replace placeholder pages with real documentation, not stubs.
- Work in small chunks. A chunk can be one page or a few closely related pages.
- After each completed chunk, you are allowed to make a git commit using conventional commit syntax, for example `docs: write caching and compression docs`.
- After you complete a TODO item, update this `PROMPT.md` file to mark it done.
- You may edit only the `## TODO` section of this file. Do not change any other part of `PROMPT.md`.
- When updating progress, only change `- [ ]` to `- [x]` for completed items. Do not rewrite, reorder, or remove items.
- Keep going until every TODO item is checked off.
- When you have completed the TODO list, delete PROMPT.md

## Writing Style

- Use direct, plain, concrete language. Prefer “SlateDB stores WAL and SST data in object storage” over vague or abstract phrasing.
- Use active voice by default.
- Keep the tone factual and engineering-oriented. Avoid hype, cheerleading, vague claims, and marketing adjectives like “powerful,” “seamless,” or “easy” unless the page immediately explains in what sense.
- Avoid filler intros such as “In this guide, we will…” unless they add real orientation value.
- Avoid repetitive phrases, sentence structures, and paragraph structures. Vary the number of sentences in a paragraph, the number of paragraphs in a section, and the number of sections in a page to create a natural flow.
- Keep page, section, and subsection titles short. For example, use "External Configuration" instead of "What stays outside SlateDB configuration".
- Prefer linking to code or other documentation rather than repeating information.
- Link to docs.rs or Github code when referencing a specific structure, function, enum, etc.
- Keep example code blocks short and to the point.
- Don't include "start here" sections or introduction paragraphs that point to related pages.
- Don't re-summarize documentation that's already available in code; link to it instead.
- Don't include "next steps" or "read next" sections.
- Don't use too many tables or bullet point lists.
- Don't include "use this page.." type guidance in pages.

## TODO
- [ ] `website/src/content/docs/docs/design/caching.mdx`
- [ ] `website/src/content/docs/docs/design/checkpoints.mdx`
- [ ] `website/src/content/docs/docs/design/clones.mdx`
- [ ] `website/src/content/docs/docs/design/compression.mdx`
- [ ] `website/src/content/docs/docs/design/time.mdx`
- [ ] `website/src/content/docs/docs/operations/compatibility.mdx`
- [ ] `website/src/content/docs/docs/operations/configuration.mdx`
- [ ] `website/src/content/docs/docs/operations/errors.mdx`
- [ ] `website/src/content/docs/docs/operations/metrics.mdx`
