# Ralph Wiggum Loop

You are in a Ralph Wiggum loop. This `PROMPT.md` file is fed back to you on each iteration. Treat it as persistent instructions plus task state.

Your goal is to write accurate SlateDB documentation for the empty pages listed in the `## TODO` section below.

Rules:
- Work on one TODO item per iteration.
- After you complete a TODO item, update this `PROMPT.md` file to mark it done.
- You may edit only the `## TODO` section of this file. Do not change any other part of `PROMPT.md`.
- You may add, update, or remove TODO items, provided they conform to the goals defined above.
- Keep going until every TODO item is checked off.
- When you have completed the TODO list, delete PROMPT.md.

## Writing Style

- Use direct, plain, concrete language. Prefer “SlateDB stores WAL and SST data in object storage” over vague or abstract phrasing.
- Use active voice by default.
- Be instructive. You're trying to teach a reader how SlateDB works (the design pages) or how to operate it (the operations pages).
- Keep the tone factual and engineering-oriented. Avoid hype, cheerleading, vague claims, and marketing adjectives like “powerful,” “seamless,” r “easy” unless the page immediately explains in what sense.
- Avoid filler intros such as “In this guide, we will…” unless they add real orientation value.
- Avoid repetitive phrases, sentence structures, and paragraph structures. Vary the number of sentences in a paragraph, the number of paragraphs n a section, and the number of sections in a page to create a natural flow.
- Keep page, section, and subsection titles short. For example, use "External Configuration" instead of "What stays outside SlateDB onfiguration".
- Prefer linking to code or other documentation rather than repeating information.
- Link to docs.rs or Github code when referencing a specific structure, function, enum, etc.
- Keep example code blocks short and to the point.
- Don't include "start here" sections or introduction paragraphs that point to related pages.
- Don't re-summarize documentation that's already available in code; link to it instead.
- Don't include "next steps" or "read next" sections.
- Don't use too many tables or bullet point lists.
- Don't include "use this page.." type guidance in pages.
- Don't use "this matters because" type phrases.
- Include mermaid diagrams when they're helpful.

## TODO
- [ ] Write `website/src/content/docs/docs/design/reader.mdx` about `DbReader`
- [ ] Write `website/src/content/docs/docs/design/sorted-string-table.mdx` about SSTs
- [ ] Write `website/src/content/docs/docs/design/block-transformer.mdx` about BlockTransformer
- [ ] Write `website/src/content/docs/docs/design/merge-operator.mdx` about MergeOperator
