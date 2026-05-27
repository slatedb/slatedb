/**
 * Tiny build-time monochrome highlighter for the landing's quickstart panes.
 *
 * Single-color scheme: language keywords get wrapped in <span class="kw">,
 * which the page CSS paints in --accent. Everything else stays default ink.
 * Not a real parser — just whole-word keyword matching. Good enough for the
 * five short snippets on the landing, and trivially auditable.
 */

const KEYWORDS: Record<string, string[]> = {
    rust: ['use', 'let', 'mut', 'fn', 'pub', 'async', 'await', 'match', 'if', 'else', 'return', 'as', 'mod', 'crate', 'self', 'Self', 'struct', 'impl', 'trait'],
    go: ['package', 'import', 'func', 'var', 'const', 'if', 'else', 'for', 'range', 'return', 'type', 'struct', 'interface', 'go', 'defer', 'chan', 'select', 'switch', 'case', 'byte', 'string', 'int', 'bool', 'error', 'nil', 'true', 'false'],
    java: ['public', 'private', 'protected', 'class', 'interface', 'void', 'return', 'new', 'final', 'static', 'import', 'package', 'extends', 'implements', 'byte', 'int', 'long', 'boolean'],
    node: ['const', 'let', 'var', 'function', 'return', 'await', 'async', 'new', 'class', 'import', 'export', 'from', 'if', 'else', 'for', 'while'],
    python: ['def', 'await', 'async', 'if', 'else', 'elif', 'for', 'while', 'return', 'import', 'from', 'as', 'class', 'with', 'lambda', 'None', 'True', 'False', 'in', 'is', 'not'],
};

const escapeHtml = (s: string): string =>
    s.replace(/[&<>"']/g, (c) =>
        ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]!,
    );

export function highlight(code: string, lang: string): string {
    const kws = KEYWORDS[lang];
    const escaped = escapeHtml(code);
    if (!kws || kws.length === 0) return escaped;
    const re = new RegExp(`\\b(${kws.join('|')})\\b`, 'g');
    return escaped.replace(re, '<span class="kw">$1</span>');
}
