/**
 * Build-time GitHub stargazer fetch with in-process caching.
 *
 * Astro re-runs page frontmatter on every dev render, so we cache at module
 * scope (which Vite reuses across HMR reloads) to avoid hammering the API
 * during development. A short TTL keeps the value reasonably fresh.
 */

const FALLBACK = '3.2k';
const TTL_MS = 60 * 60 * 1000;

let cached: { value: string; ts: number } | undefined;

const format = (stars: number): string => `${(stars / 1000).toFixed(1)}k`;

export async function getStarsLabel(): Promise<string> {
    const now = Date.now();
    if (cached && now - cached.ts < TTL_MS) return cached.value;

    let value = FALLBACK;
    try {
        const r = await fetch('https://api.github.com/repos/slatedb/slatedb', {
            headers: { 'User-Agent': 'slatedb-site-build' },
            signal: AbortSignal.timeout(3000),
        });
        if (r.ok) {
            const data = (await r.json()) as { stargazers_count?: number };
            if (typeof data.stargazers_count === 'number') {
                value = format(data.stargazers_count);
            }
        }
    } catch {
        // network error / timeout — keep the fallback
    }

    cached = { value, ts: now };
    return value;
}
