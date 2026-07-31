import { visit } from 'unist-util-visit';

// Container directives we recognize, mapped to their default title. Add more
// entries here (e.g. tip, warning, danger) to support them later.
const ADMONITIONS = {
	note: 'Note',
};

/**
 * Turn `:::note ... :::` container directives into a styled <aside>:
 *
 *   :::note
 *   Body text.
 *   :::
 *
 *   :::note[Custom title]
 *   Body text.
 *   :::
 *
 * Renders as <aside class="admonition admonition-note"> with a leading
 * <p class="admonition-title">. Requires `remark-directive` to run first so the
 * directive nodes exist. Only known names are transformed; unknown directives
 * are left untouched.
 */
export default function remarkAdmonitions() {
	return (tree, file) => {
		// Scope to the blog. Starlight docs share this remark pipeline but have
		// their own native `:::note` asides — transforming those here would
		// hijack them, so only touch files under src/content/blog/.
		const filePath = file?.path ?? file?.history?.[0] ?? '';
		if (!filePath.replaceAll('\\', '/').includes('/content/blog/')) return;

		visit(tree, 'containerDirective', (node) => {
			const defaultTitle = ADMONITIONS[node.name];
			if (!defaultTitle) return;

			const data = node.data ?? (node.data = {});
			data.hName = 'aside';
			data.hProperties = {
				className: ['admonition', `admonition-${node.name}`],
			};

			// Optional custom title: `:::note[My title]` lands as a leading
			// paragraph flagged `directiveLabel`. Lift its children into the title
			// and drop it from the body; otherwise fall back to the default title.
			let titleChildren = [{ type: 'text', value: defaultTitle }];
			const first = node.children[0];
			if (first && first.type === 'paragraph' && first.data?.directiveLabel) {
				titleChildren = first.children;
				node.children.shift();
			}

			node.children.unshift({
				type: 'paragraph',
				data: { hName: 'p', hProperties: { className: ['admonition-title'] } },
				children: titleChildren,
			});
		});
	};
}
