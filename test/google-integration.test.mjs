import test from 'node:test';
import assert from 'node:assert/strict';
import worker from '../src/worker.mjs';
import { getAccessToken } from '../src/google.mjs';
import { getYoutubeAccessToken } from '../src/services/youtubeAuth.mjs';
import { fetchPerformanceMetrics } from '../src/gmb.mjs';
import { validOAuthState } from '../src/oauthState.mjs';

function environment(initial = {}) {
    const values = new Map(Object.entries(initial).map(([key, value]) => [key, JSON.stringify(value)]));
    const writes = [];
    return {
        GOOGLE_CLIENT_ID: 'test-client', GOOGLE_CLIENT_SECRET: 'test-secret',
        GOOGLE_REDIRECT_URI: 'https://app.test/oauth2callback', values, writes,
        D1_DB: { prepare(sql) {
            let args = [];
            return {
                bind(...input) { args = input; return this; },
                async run() {
                    writes.push({ sql, args });
                    if (sql.includes('INSERT INTO kv')) values.set(args[0], args[1]);
                    return { success: true };
                },
                async first() { return values.has(args[0]) ? { value: values.get(args[0]) } : null; }
            };
        } }
    };
}
const fresh = () => ({ access_token: 'access', refresh_token: 'refresh', expiry_date: Date.now() + 3600000 });
function mockFetch(t, fn) { t.mock.method(globalThis, 'fetch', fn); }

for (const [flow, startPath, callbackPath] of [
    ['gbp', '/auth', '/oauth2callback'],
    ['youtube', '/api/google/connect-youtube', '/api/google/oauth/callback']
]) {
    test(`${flow}: OAuth is browser-bound; missing, mismatched and expired state rejected`, async t => {
        const env = environment();
        mockFetch(t, () => { throw new Error('Must not exchange invalid callbacks'); });
        const start = await worker.fetch(new Request(`https://app.test${startPath}`), env, {});
        assert.equal(start.status, 302);
        const state = new URL(start.headers.get('Location')).searchParams.get('state');
        const cookie = start.headers.get('Set-Cookie');
        assert.match(cookie, /HttpOnly; SameSite=Lax; Max-Age=600; Secure/);
        assert.match(cookie, /__Host-/);
        const callback = new Request(`https://app.test${callbackPath}?state=${state}&error=access_denied`, { headers: { Cookie: cookie.split(';')[0] } });
        assert.equal(validOAuthState(callback, flow), true);
        assert.equal(validOAuthState(callback, flow === 'gbp' ? 'youtube' : 'gbp'), false);
        const denied = await worker.fetch(callback, env, {});
        assert.match(await denied.text(), /access_denied/);
        assert.match(denied.headers.get('Set-Cookie'), /Max-Age=0/);
        for (const bad of ['', '?code=bad', '?state=mismatch&code=bad']) {
            const response = await worker.fetch(new Request(`https://app.test${callbackPath}${bad}`), env, {});
            assert.equal(response.status, 400);
            assert.match(await response.text(), /Invalid or expired/);
        }
        const expired = `${Date.now() - 601000}.${'a'.repeat(64)}`;
        assert.equal(validOAuthState(new Request(`https://app.test${callbackPath}?state=${expired}`, {
            headers: { Cookie: `__Host-google_oauth_${flow}=${expired}` }
        }), flow), false);
    });
}

test('GBP valid callback exchanges code and clears the session without logging secrets', async t => {
    const env = environment();
    const logs = [];
    t.mock.method(console, 'log', (...args) => logs.push(args.join(' ')));
    mockFetch(t, async (url, init) => {
        assert.equal(url, 'https://oauth2.googleapis.com/token');
        assert.equal(init.body.get('code'), 'test-code');
        return Response.json({ access_token: 'secret-access', refresh_token: 'secret-refresh', expires_in: 3600 });
    });
    const start = await worker.fetch(new Request('https://app.test/auth'), env, {});
    const state = new URL(start.headers.get('Location')).searchParams.get('state');
    const response = await worker.fetch(new Request(`https://app.test/oauth2callback?code=test-code&state=${state}`, {
        headers: { Cookie: start.headers.get('Set-Cookie').split(';')[0] }
    }), env, {});
    assert.equal(response.status, 200);
    assert.match(response.headers.get('Set-Cookie'), /Max-Age=0/);
    assert.equal(JSON.parse(env.values.get('tokens')).refresh_token, 'secret-refresh');
    assert.equal(logs.length, 0);
});

test('accounts uses the documented Account Management endpoint', async t => {
    const env = environment({ tokens: fresh() });
    mockFetch(t, async (url, init) => {
        assert.equal(url, 'https://mybusinessaccountmanagement.googleapis.com/v1/accounts');
        assert.equal(init.headers.get('Authorization'), 'Bearer access');
        return Response.json({ accounts: [{ name: 'accounts/123' }] });
    });
    const response = await worker.fetch(new Request('https://app.test/accounts'), env, {});
    assert.deepEqual(await response.json(), { accounts: [{ name: 'accounts/123' }] });
});

test('valid GBP tokens are reused without refresh', async t => {
    mockFetch(t, () => { throw new Error('Unexpected refresh'); });
    assert.equal(await getAccessToken(environment({ tokens: fresh() })), 'access');
});

test('GBP refresh preserves the refresh token and updates expiry', async t => {
    const env = environment({ tokens: { ...fresh(), expiry_date: 1 } });
    mockFetch(t, async () => Response.json({ access_token: 'new-access', expires_in: 3600 }));
    assert.equal(await getAccessToken(env), 'new-access');
    const saved = JSON.parse(env.values.get('tokens'));
    assert.equal(saved.refresh_token, 'refresh');
    assert.ok(saved.expiry_date > Date.now());
});

test('GBP expired/revoked credentials and temporary refresh errors are reported', async t => {
    let status = 400;
    mockFetch(t, async () => Response.json({ error: status === 400 ? 'invalid_grant' : 'temporarily_unavailable' }, { status }));
    const env = environment({ tokens: { ...fresh(), expiry_date: 1 } });
    await assert.rejects(getAccessToken(env), /reconnect Google/);
    status = 503;
    await assert.rejects(getAccessToken(env), /try again later/);
    await assert.rejects(getAccessToken(environment({ tokens: { access_token: 'expired', expiry_date: 1 } })), /reconnect Google/);
});

test('YouTube transient errors do not permanently mark channels for reconnection', async t => {
    let status = 503;
    mockFetch(t, async () => Response.json({ error: status === 400 ? 'invalid_grant' : 'temporarily_unavailable' }, { status }));
    const env = environment();
    const row = { id: 'channel', token_expiry: 1, refresh_token: 'refresh' };
    await assert.rejects(getYoutubeAccessToken(env, row), /try again later/);
    assert.equal(env.writes.length, 0);
    status = 400;
    await assert.rejects(getYoutubeAccessToken(env, row), /reconnect channel/);
    assert.ok(env.writes.some(write => write.sql.includes('reconnect_required = 1')));
});

test('keyword performance follows page tokens and retains the response format', async t => {
    const env = environment({ tokens: fresh(), profiles: [{ profileId: 'p1', locationId: '123' }] });
    const pages = [];
    mockFetch(t, async url => {
        const parsed = new URL(url);
        if (parsed.pathname.endsWith(':fetchMultiDailyMetricsTimeSeries')) return Response.json({});
        assert.match(parsed.pathname, /locations\/123\/searchkeywords\/impressions\/monthly$/);
        pages.push(parsed.searchParams.get('pageToken'));
        if (pages.length === 1) return Response.json({ searchKeywordsCounts: [{ searchKeyword: 'first', insightsValue: { value: '42' } }], nextPageToken: 'next+/=' });
        assert.equal(parsed.searchParams.get('pageToken'), 'next+/=');
        return Response.json({ searchKeywordsCounts: [{ searchKeyword: 'second', insightsValue: { value: '20' } }] });
    });
    const result = await fetchPerformanceMetrics(env, 'p1');
    assert.equal(result.keywordError, '');
    assert.deepEqual(result.keywords.items.map(x => x.keyword), ['first', 'second']);
    assert.equal(result.keywords.items[0].value, 42);
    assert.equal(pages.length, 2);
});
