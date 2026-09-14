import { getJson, setJson } from './storage.mjs';

export const BLOG_MONITOR_KEY = 'blogMonitor';
const INDEX = 'https://epfproservices.com/blog/';
const decode = value => String(value || '').replace(/&amp;/g, '&').replace(/&quot;/g, '"').replace(/&#(?:39|x27);/g, "'").replace(/&lt;/g, '<').replace(/&gt;/g, '>');

export function articleUrls(html) {
    return [...new Set([...html.matchAll(/href=["']([^"']+)["']/gi)].flatMap(([, href]) => {
        try {
            const url = new URL(decode(href), INDEX);
            return url.origin === new URL(INDEX).origin && /^\/blog\/[^/]+\/?$/.test(url.pathname)
                ? [`${url.origin}${url.pathname.replace(/\/$/, '')}/`] : [];
        } catch { return []; }
    }))];
}

export function articlePayload(html, url) {
    const metadata = {};
    for (const [tag] of html.matchAll(/<meta\b[^>]*>/gi)) {
        const attrs = Object.fromEntries([...tag.matchAll(/([\w:-]+)\s*=\s*(["'])(.*?)\2/gs)].map(([, key, , value]) => [key.toLowerCase(), decode(value)]));
        metadata[attrs.property || attrs.name] = attrs.content;
    }
    const title = metadata['og:title'] || decode(html.match(/<title[^>]*>(.*?)<\/title>/is)?.[1]);
    if (!title) throw new Error('Live article has no title');
    return { event: 'BLOG_POST_CREATED', url, title, excerpt: metadata.description || metadata['og:description'] || '', publishedAt: metadata['article:published_time'] || '' };
}

async function readLive(url, fetcher) {
    const response = await fetcher(url, { redirect: 'manual', signal: AbortSignal.timeout(20000), headers: { 'Cache-Control': 'no-cache' } });
    if (!response.ok) throw new Error(`Blog fetch returned HTTP ${response.status}`);
    const html = await response.text();
    if (html.length > 4000000) throw new Error('Blog response exceeds size limit');
    return html;
}

export async function claimBlogDelivery(env, url) {
    await env.D1_DB.prepare('CREATE TABLE IF NOT EXISTS blog_deliveries (url TEXT PRIMARY KEY, status TEXT NOT NULL, updated_at TEXT NOT NULL)').run();
    const result = await env.D1_DB.prepare("INSERT OR IGNORE INTO blog_deliveries (url,status,updated_at) VALUES (?1,'PROCESSING',?2)").bind(url, new Date().toISOString()).run();
    return result.meta.changes === 1;
}
export async function finishBlogDelivery(env, url, status) {
    await env.D1_DB.prepare('UPDATE blog_deliveries SET status=?2, updated_at=?3 WHERE url=?1').bind(url, status, new Date().toISOString()).run();
}

// A durable baseline prevents a first-run backlog. Delivery claims also cover incoming webhooks.
export async function runBlogMonitor(env, deliver, fetcher = fetch) {
    if (env.BLOG_MONITOR_ENABLED !== 'true') return;
    const previous = await getJson(env, BLOG_MONITOR_KEY, null);
    const state = { ...previous, enabled: true, checkedAt: new Date().toISOString(), error: '' };
    try {
        if (!env.EPF_WEBHOOK_SECRET) throw new Error('Blog webhook secret is missing');
        const urls = articleUrls(await readLive(INDEX, fetcher));
        if (!urls.length) throw new Error('No articles found on the live blog; baseline unchanged');
        if (!previous?.initializedAt) {
            state.initializedAt = state.checkedAt;
            state.seen = urls;
            state.baselineCount = urls.length;
            state.lastResult = 'Existing articles recorded. Waiting for newly published articles.';
        } else {
            const seen = new Set(previous.seen || []);
            const pending = urls.filter(url => !seen.has(url));
            state.pendingCount = pending.length;
            // One delivery per tick keeps image generation and Google requests bounded.
            for (const url of pending.slice(0, 1)) {
                const payload = articlePayload(await readLive(url, fetcher), url);
                const response = await deliver(new Request('https://internal/api/webhooks/blog-created', {
                    method: 'POST', headers: { 'Content-Type': 'application/json', 'x-epf-webhook-secret': env.EPF_WEBHOOK_SECRET }, body: JSON.stringify(payload)
                }), env);
                const result = await response.json();
                state.lastDelivery = { url, checkedAt: state.checkedAt, status: result.data?.status || result.duplicate?.status || 'NEEDS_REVIEW', error: result.error || '' };
                if (response.ok || response.status === 409) {
                    seen.add(url);
                    state.pendingCount--;
                } else {
                    throw new Error(result.data?.postError || result.error || `Delivery HTTP ${response.status}`);
                }
            }
            state.seen = [...seen];
            state.lastResult = pending.length ? 'New article delivery checked.' : 'No new articles. Automatic checks are running.';
        }
    } catch (error) {
        state.error = error.message || String(error);
    }
    await setJson(env, BLOG_MONITOR_KEY, state);
    return state;
}

export async function blogDeliveryIssues(env) {
    await env.D1_DB.prepare('CREATE TABLE IF NOT EXISTS blog_deliveries (url TEXT PRIMARY KEY, status TEXT NOT NULL, updated_at TEXT NOT NULL)').run();
    const rows = await env.D1_DB.prepare("SELECT url,status,updated_at FROM blog_deliveries WHERE status='NEEDS_REVIEW' OR (status='PROCESSING' AND updated_at < ?1) ORDER BY updated_at DESC LIMIT 20").bind(new Date(Date.now() - 15 * 60000).toISOString()).all();
    return rows.results || [];
}
