import test from 'node:test';
import assert from 'node:assert/strict';
import { articleUrls, articlePayload, runBlogMonitor, claimBlogDelivery, finishBlogDelivery } from '../src/blogMonitor.mjs';
function env() {
    const kv = new Map(), claims = new Map();
    return { BLOG_MONITOR_ENABLED: 'true', EPF_WEBHOOK_SECRET: 'test', kv, claims,
        D1_DB: { prepare(sql) { let args; return {
            bind(...a) { args = a; return this; },
            async first() { return kv.has(args[0]) ? { value: kv.get(args[0]) } : null; },
            async run() {
                if (sql.includes('INSERT INTO kv')) kv.set(args[0],args[1]);
                if (sql.includes('INSERT OR IGNORE INTO blog_deliveries')) {
                    if (claims.has(args[0])) return { meta: { changes: 0 } };
                    claims.set(args[0], 'PROCESSING'); return { meta: { changes: 1 } };
                }
                if (sql.includes('UPDATE blog_deliveries')) claims.set(args[0],args[1]);
                return { meta: { changes: 1 } };
            }
        }; } }
    };
}
const listing = slugs => slugs.map(slug => `<a href="/blog/${slug}/">Article</a>`).join('');
const page = '<meta content="Test &amp; title" property="og:title"><meta name="description" content="Article summary">';
test('discovery restricts domain and article paths; metadata handles attribute order and entities', () => {
    assert.deepEqual(articleUrls(listing(['one','one']) + '<a href="https://evil.test/blog/two/">x</a><a href="/blog/">index</a>'), ['https://epfproservices.com/blog/one/']);
    assert.equal(articlePayload(page, 'url').title, 'Test & title');
    assert.throws(() => articlePayload('no metadata','url'));
});
test('first scan baselines existing articles, new live article delivered once, removed articles stay seen', async () => {
    const e = env(); let slugs = ['old'], deliveries = [];
    const fetcher = async url => new Response(url.endsWith('/blog/') ? listing(slugs) : page);
    const deliver = async request => { deliveries.push(await request.json()); return Response.json({ data: { status: 'POSTED' } }); };
    const initial = await runBlogMonitor(e,deliver,fetcher);
    assert.equal(initial.baselineCount,1); assert.equal(deliveries.length,0);
    slugs = ['new','old']; await runBlogMonitor(e,deliver,fetcher); await runBlogMonitor(e,deliver,fetcher);
    assert.equal(deliveries.length,1); assert.equal(deliveries[0].title,'Test & title');
    slugs=['old']; await runBlogMonitor(e,deliver,fetcher);
    slugs=['new','old']; await runBlogMonitor(e,deliver,fetcher); assert.equal(deliveries.length,1);
});
test('failed website fetch preserves baseline and retries discovery', async () => {
    const e = env(); const deliver = () => { throw Error('Unexpected publish'); };
    await runBlogMonitor(e,deliver,async () => new Response(listing(['old'])));
    const result = await runBlogMonitor(e,deliver,async () => new Response('failure',{status:503}));
    assert.match(result.error,/503/); assert.equal(result.seen.length,1);
});
test('durable claim blocks concurrent and ambiguous repeat delivery', async () => {
    const e = env();
    const results = await Promise.all([claimBlogDelivery(e,'url'),claimBlogDelivery(e,'url')]);
    assert.deepEqual(results.sort(),[false,true]);
    await finishBlogDelivery(e,'url','NEEDS_REVIEW');
    assert.equal(await claimBlogDelivery(e,'url'),false);
});
test('disabled monitor does not fetch or deliver', async () => {
    await runBlogMonitor({ BLOG_MONITOR_ENABLED:'false' }, () => assert.fail(), () => assert.fail());
});
test('ambiguous delivery is visible and a durable duplicate stops repeated publication attempts', async () => {
    const e = env(); let slugs=['old']; let calls=0;
    const fetcher=async url => new Response(url.endsWith('/blog/') ? listing(slugs) : page);
    const deliver=async () => ++calls === 1
        ? Response.json({ error:'GBP auto-post failed',data:{status:'POST_FAILED'} },{status:500})
        : Response.json({ error:'Duplicate blog url',duplicate:{status:'CLAIMED_OR_NEEDS_REVIEW'} },{status:409});
    await runBlogMonitor(e,deliver,fetcher); slugs=['new','old'];
    let state=await runBlogMonitor(e,deliver,fetcher); assert.match(state.error,/failed/);
    state=await runBlogMonitor(e,deliver,fetcher); assert.equal(state.lastDelivery.status,'CLAIMED_OR_NEEDS_REVIEW');
    await runBlogMonitor(e,deliver,fetcher); assert.equal(calls,2);
});
test('redirects are not followed and do not replace the baseline', async () => {
    const e=env(); const state=await runBlogMonitor(e,()=>assert.fail(),async (url,options)=>{
        assert.equal(options.redirect,'manual');
        return new Response(null,{status:302,headers:{Location:'https://other.test/'}});
    });
    assert.match(state.error,/302/); assert.equal(state.initializedAt,undefined);
});
test('multiple new articles are delivered over successive checks, one per tick', async () => {
    const e=env(); let slugs=['old'], calls=0;
    const fetcher=async url=>new Response(url.endsWith('/blog/')?listing(slugs):page);
    const deliver=async()=>{calls++;return Response.json({data:{status:'POSTED'}});};
    await runBlogMonitor(e,deliver,fetcher); slugs=['one','two','old'];
    let state=await runBlogMonitor(e,deliver,fetcher); assert.equal(calls,1);assert.equal(state.pendingCount,1);
    state=await runBlogMonitor(e,deliver,fetcher); assert.equal(calls,2);assert.equal(state.pendingCount,0);
});
