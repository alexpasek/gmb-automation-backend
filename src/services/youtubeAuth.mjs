const YOUTUBE_SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube.readonly"
];

function nowIso() {
    return new Date().toISOString();
}

function randomId(prefix = "yt") {
    if (typeof crypto !== "undefined" && crypto.randomUUID) return `${prefix}-${crypto.randomUUID()}`;
    return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
}

async function deriveKey(env) {
    const secret = env.YOUTUBE_TOKEN_ENCRYPTION_KEY || env.TOKEN_ENCRYPTION_KEY || env.GOOGLE_CLIENT_SECRET;
    if (!secret) throw new Error("Missing YOUTUBE_TOKEN_ENCRYPTION_KEY or GOOGLE_CLIENT_SECRET for token storage");
    const bytes = new TextEncoder().encode(String(secret));
    const digest = await crypto.subtle.digest("SHA-256", bytes);
    return crypto.subtle.importKey("raw", digest, "AES-GCM", false, ["encrypt", "decrypt"]);
}

async function encryptToken(env, value) {
    const text = String(value || "");
    if (!text) return "";
    const iv = crypto.getRandomValues(new Uint8Array(12));
    const key = await deriveKey(env);
    const encrypted = await crypto.subtle.encrypt({ name: "AES-GCM", iv }, key, new TextEncoder().encode(text));
    const joined = new Uint8Array(iv.length + encrypted.byteLength);
    joined.set(iv, 0);
    joined.set(new Uint8Array(encrypted), iv.length);
    return "enc:" + btoa(String.fromCharCode(...joined));
}

async function decryptToken(env, value) {
    const raw = String(value || "");
    if (!raw) return "";
    if (!raw.startsWith("enc:")) return raw;
    const bytes = Uint8Array.from(atob(raw.slice(4)), (c) => c.charCodeAt(0));
    const iv = bytes.slice(0, 12);
    const data = bytes.slice(12);
    const key = await deriveKey(env);
    const decrypted = await crypto.subtle.decrypt({ name: "AES-GCM", iv }, key, data);
    return new TextDecoder().decode(decrypted);
}

export async function ensureYoutubeTables(env) {
    await env.D1_DB.prepare(`
    CREATE TABLE IF NOT EXISTS youtube_channels (
      id TEXT PRIMARY KEY,
      channel_id TEXT NOT NULL UNIQUE,
      channel_title TEXT NOT NULL,
      access_token TEXT NOT NULL,
      refresh_token TEXT,
      token_expiry INTEGER,
      scope TEXT,
      reconnect_required INTEGER DEFAULT 0,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
  `).run();

    await env.D1_DB.prepare(`
    CREATE TABLE IF NOT EXISTS youtube_video_posts (
      id TEXT PRIMARY KEY,
      channel_db_id TEXT,
      channel_id TEXT,
      youtube_video_id TEXT,
      youtube_url TEXT,
      title TEXT,
      description TEXT,
      tags TEXT,
      hashtags TEXT,
      service TEXT,
      city TEXT,
      neighbourhoods TEXT,
      video_type TEXT,
      website_url TEXT,
      utm_url TEXT,
      privacy_status TEXT,
      publish_status TEXT,
      thumbnail_status TEXT,
      gbp_cross_post_status TEXT,
      gbp_result TEXT,
      error_message TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
  `).run();

    await env.D1_DB.prepare(`
    CREATE TABLE IF NOT EXISTS youtube_community_posts (
      id TEXT PRIMARY KEY,
      channel_db_id TEXT,
      channel_id TEXT,
      post_text TEXT NOT NULL,
      image_url TEXT,
      service TEXT,
      city TEXT,
      neighbourhoods TEXT,
      post_type TEXT,
      website_url TEXT,
      utm_url TEXT,
      hashtags TEXT,
      status TEXT NOT NULL,
      posted_at TEXT,
      error_message TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
  `).run();
}

export function buildYoutubeAuthUrl(request, env) {
    const origin = new URL(request.url).origin;
    const redirectUri = env.YOUTUBE_REDIRECT_URI || `${origin}/api/google/oauth/callback`;
    const params = new URLSearchParams({
        client_id: env.GOOGLE_CLIENT_ID,
        redirect_uri: redirectUri,
        response_type: "code",
        access_type: "offline",
        prompt: "consent",
        include_granted_scopes: "true",
        state: "youtube",
        scope: YOUTUBE_SCOPES.join(" ")
    });
    return "https://accounts.google.com/o/oauth2/v2/auth?" + params.toString();
}

async function exchangeCode(request, env, code) {
    const origin = new URL(request.url).origin;
    const redirectUri = env.YOUTUBE_REDIRECT_URI || `${origin}/api/google/oauth/callback`;
    const body = new URLSearchParams({
        code,
        client_id: env.GOOGLE_CLIENT_ID,
        client_secret: env.GOOGLE_CLIENT_SECRET,
        redirect_uri: redirectUri,
        grant_type: "authorization_code"
    });
    const resp = await fetch("https://oauth2.googleapis.com/token", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body
    });
    if (!resp.ok) throw new Error("YouTube token exchange failed: " + await resp.text());
    const tokens = await resp.json();
    if (typeof tokens.expires_in === "number") tokens.token_expiry = Date.now() + tokens.expires_in * 1000;
    return tokens;
}

async function fetchMine(accessToken) {
    const resp = await fetch("https://www.googleapis.com/youtube/v3/channels?part=snippet&mine=true", {
        headers: { Authorization: `Bearer ${accessToken}` }
    });
    if (!resp.ok) throw new Error("Failed to read YouTube channel: " + await resp.text());
    const data = await resp.json();
    const channel = Array.isArray(data.items) ? data.items[0] : null;
    if (!channel || !channel.id) throw new Error("No YouTube channel found for this Google account");
    return {
        channelId: channel.id,
        channelTitle: channel.snippet?.title || channel.id
    };
}

export async function saveYoutubeConnection(request, env, code) {
    await ensureYoutubeTables(env);
    const tokens = await exchangeCode(request, env, code);
    const channel = await fetchMine(tokens.access_token);
    const existing = await env.D1_DB.prepare("SELECT id, refresh_token FROM youtube_channels WHERE channel_id = ?1")
        .bind(channel.channelId)
        .first();
    const id = existing?.id || randomId("ytch");
    const refreshToken = tokens.refresh_token ? await encryptToken(env, tokens.refresh_token) : existing?.refresh_token || "";
    const accessToken = await encryptToken(env, tokens.access_token);
    const now = nowIso();

    await env.D1_DB.prepare(`
      INSERT INTO youtube_channels (
        id, channel_id, channel_title, access_token, refresh_token, token_expiry, scope,
        reconnect_required, created_at, updated_at
      )
      VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, ?8, ?8)
      ON CONFLICT(channel_id) DO UPDATE SET
        channel_title = excluded.channel_title,
        access_token = excluded.access_token,
        refresh_token = excluded.refresh_token,
        token_expiry = excluded.token_expiry,
        scope = excluded.scope,
        reconnect_required = 0,
        updated_at = excluded.updated_at
    `).bind(
        id,
        channel.channelId,
        channel.channelTitle,
        accessToken,
        refreshToken,
        tokens.token_expiry || null,
        tokens.scope || YOUTUBE_SCOPES.join(" "),
        now
    ).run();

    return { id, channelId: channel.channelId, channelTitle: channel.channelTitle };
}

export async function listYoutubeChannels(env) {
    await ensureYoutubeTables(env);
    const { results } = await env.D1_DB.prepare(`
      SELECT id, channel_id, channel_title, token_expiry, reconnect_required, created_at, updated_at
      FROM youtube_channels
      ORDER BY updated_at DESC
    `).all();
    return results || [];
}

export async function getYoutubeChannelForUse(env, id) {
    await ensureYoutubeTables(env);
    const row = await env.D1_DB.prepare("SELECT * FROM youtube_channels WHERE id = ?1 OR channel_id = ?1")
        .bind(String(id || ""))
        .first();
    if (!row) throw new Error("YouTube channel not connected");
    if (row.reconnect_required) throw new Error("YouTube channel reconnect required");
    return row;
}

async function markReconnectRequired(env, id, message) {
    await env.D1_DB.prepare("UPDATE youtube_channels SET reconnect_required = 1, updated_at = ?2 WHERE id = ?1")
        .bind(id, nowIso())
        .run();
    throw new Error(message || "YouTube reconnect required");
}

export async function getYoutubeAccessToken(env, channelRow) {
    const row = channelRow;
    const expiry = Number(row.token_expiry || 0);
    if (expiry && expiry - 60000 > Date.now()) {
        return decryptToken(env, row.access_token);
    }

    const refreshToken = await decryptToken(env, row.refresh_token || "");
    if (!refreshToken) {
        await markReconnectRequired(env, row.id, "Missing YouTube refresh token; reconnect channel");
    }

    const body = new URLSearchParams({
        client_id: env.GOOGLE_CLIENT_ID,
        client_secret: env.GOOGLE_CLIENT_SECRET,
        refresh_token: refreshToken,
        grant_type: "refresh_token"
    });
    const resp = await fetch("https://oauth2.googleapis.com/token", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body
    });
    if (!resp.ok) {
        await markReconnectRequired(env, row.id, "YouTube token refresh failed: " + await resp.text());
    }
    const data = await resp.json();
    const encryptedAccess = await encryptToken(env, data.access_token);
    const tokenExpiry = typeof data.expires_in === "number" ? Date.now() + data.expires_in * 1000 : null;
    await env.D1_DB.prepare(`
      UPDATE youtube_channels
      SET access_token = ?2, token_expiry = ?3, reconnect_required = 0, updated_at = ?4
      WHERE id = ?1
    `).bind(row.id, encryptedAccess, tokenExpiry, nowIso()).run();
    return data.access_token;
}

export async function insertYoutubeVideoRecord(env, payload) {
    await ensureYoutubeTables(env);
    const id = payload.id || randomId("ytpost");
    const now = nowIso();
    await env.D1_DB.prepare(`
      INSERT INTO youtube_video_posts (
        id, channel_db_id, channel_id, youtube_video_id, youtube_url, title, description,
        tags, hashtags, service, city, neighbourhoods, video_type, website_url, utm_url,
        privacy_status, publish_status, thumbnail_status, gbp_cross_post_status, gbp_result,
        error_message, created_at, updated_at
      )
      VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?22)
    `).bind(
        id,
        payload.channelDbId || "",
        payload.channelId || "",
        payload.youtubeVideoId || "",
        payload.youtubeUrl || "",
        payload.title || "",
        payload.description || "",
        JSON.stringify(payload.tags || []),
        JSON.stringify(payload.hashtags || []),
        payload.service || "",
        payload.city || "",
        JSON.stringify(payload.neighbourhoods || []),
        payload.videoType || "",
        payload.websiteUrl || "",
        payload.utmUrl || "",
        payload.privacyStatus || "",
        payload.publishStatus || "PENDING",
        payload.thumbnailStatus || "",
        payload.gbpCrossPostStatus || "",
        payload.gbpResult ? JSON.stringify(payload.gbpResult) : "",
        payload.errorMessage || "",
        now
    ).run();
    return id;
}

export async function updateYoutubeVideoRecord(env, id, patch = {}) {
    const current = await env.D1_DB.prepare("SELECT * FROM youtube_video_posts WHERE id = ?1").bind(id).first();
    if (!current) return null;
    const next = { ...current, ...patch };
    await env.D1_DB.prepare(`
      UPDATE youtube_video_posts SET
        youtube_video_id = ?2, youtube_url = ?3, publish_status = ?4, thumbnail_status = ?5,
        gbp_cross_post_status = ?6, gbp_result = ?7, error_message = ?8, updated_at = ?9
      WHERE id = ?1
    `).bind(
        id,
        next.youtube_video_id || next.youtubeVideoId || "",
        next.youtube_url || next.youtubeUrl || "",
        next.publish_status || next.publishStatus || "",
        next.thumbnail_status || next.thumbnailStatus || "",
        next.gbp_cross_post_status || next.gbpCrossPostStatus || "",
        typeof next.gbp_result === "string" ? next.gbp_result : JSON.stringify(next.gbpResult || {}),
        next.error_message || next.errorMessage || "",
        nowIso()
    ).run();
    return next;
}

export async function insertYoutubeCommunityDraft(env, payload) {
    await ensureYoutubeTables(env);
    const id = payload.id || randomId("ytcom");
    const now = nowIso();
    await env.D1_DB.prepare(`
      INSERT INTO youtube_community_posts (
        id, channel_db_id, channel_id, post_text, image_url, service, city,
        neighbourhoods, post_type, website_url, utm_url, hashtags, status,
        posted_at, error_message, created_at, updated_at
      )
      VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?16)
    `).bind(
        id,
        payload.channelDbId || "",
        payload.channelId || "",
        payload.postText || "",
        payload.imageUrl || "",
        payload.service || "",
        payload.city || "",
        JSON.stringify(payload.neighbourhoods || []),
        payload.postType || "community_post",
        payload.websiteUrl || "",
        payload.utmUrl || "",
        JSON.stringify(payload.hashtags || []),
        payload.status || "DRAFT",
        payload.postedAt || "",
        payload.errorMessage || "",
        now
    ).run();
    return id;
}

export async function listYoutubeCommunityDrafts(env, limit = 50) {
    await ensureYoutubeTables(env);
    const safeLimit = Math.max(1, Math.min(100, parseInt(limit, 10) || 50));
    const { results } = await env.D1_DB.prepare(`
      SELECT *
      FROM youtube_community_posts
      ORDER BY created_at DESC
      LIMIT ?1
    `).bind(safeLimit).all();
    return (results || []).map((row) => ({
        ...row,
        neighbourhoods: (() => {
            try { return JSON.parse(row.neighbourhoods || "[]"); } catch { return []; }
        })(),
        hashtags: (() => {
            try { return JSON.parse(row.hashtags || "[]"); } catch { return []; }
        })()
    }));
}

export async function markYoutubeCommunityDraftPosted(env, id) {
    await ensureYoutubeTables(env);
    const now = nowIso();
    await env.D1_DB.prepare(`
      UPDATE youtube_community_posts
      SET status = 'POSTED', posted_at = ?2, updated_at = ?2
      WHERE id = ?1
    `).bind(id, now).run();
    const row = await env.D1_DB.prepare("SELECT * FROM youtube_community_posts WHERE id = ?1").bind(id).first();
    return row;
}
