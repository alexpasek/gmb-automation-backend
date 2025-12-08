import { getJson, setJson } from "./storage.mjs";

const TOKEN_KEY = "tokens";

export function buildAuthUrl(env) {
    const redirectUri = env.GOOGLE_REDIRECT_URI;
    const params = new URLSearchParams({
        client_id: env.GOOGLE_CLIENT_ID,
        redirect_uri: redirectUri,
        response_type: "code",
        access_type: "offline",
        prompt: "consent",
        scope: "https://www.googleapis.com/auth/business.manage"
    });
    return "https://accounts.google.com/o/oauth2/v2/auth?" + params.toString();
}

export async function saveTokens(env, tokens) {
    await setJson(env, TOKEN_KEY, tokens || {});
}

export async function exchangeCodeForTokens(env, code) {
    const body = new URLSearchParams({
        code,
        client_id: env.GOOGLE_CLIENT_ID,
        client_secret: env.GOOGLE_CLIENT_SECRET,
        redirect_uri: env.GOOGLE_REDIRECT_URI,
        grant_type: "authorization_code"
    });

    const resp = await fetch("https://oauth2.googleapis.com/token", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body
    });

    if (!resp.ok) {
        const text = await resp.text();
        throw new Error("Token exchange failed: " + text);
    }

    const tokens = await resp.json();
    const now = Date.now();
    if (typeof tokens.expires_in === "number") {
        tokens.expiry_date = now + tokens.expires_in * 1000;
    }

    // Debug: log tokens JSON for production debugging (remove if noisy)
    console.log("TOKENS_JSON_FOR_PROD = " + JSON.stringify(tokens));

    await saveTokens(env, tokens);
    return tokens;
}

async function getTokens(env) {
    const tokens = (await getJson(env, TOKEN_KEY, null)) || null;
    return tokens;
}

async function refreshAccessTokenIfNeeded(env, tokens) {
    if (!tokens) return null;
    const now = Date.now();
    if (tokens.expiry_date && tokens.expiry_date - 60000 > now) {
        return tokens; // still valid
    }
    if (!tokens.refresh_token) return tokens;

    const body = new URLSearchParams({
        client_id: env.GOOGLE_CLIENT_ID,
        client_secret: env.GOOGLE_CLIENT_SECRET,
        refresh_token: tokens.refresh_token,
        grant_type: "refresh_token"
    });

    const resp = await fetch("https://oauth2.googleapis.com/token", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body
    });

    if (!resp.ok) {
        const text = await resp.text();
        console.error("Refresh token failed:", text);
        return tokens;
    }

    const data = await resp.json();
    const merged = {...tokens, ...data };
    if (typeof data.expires_in === "number") {
        merged.expiry_date = Date.now() + data.expires_in * 1000;
    }
    await saveTokens(env, merged);
    return merged;
}

export async function getAccessToken(env) {
    let tokens = await getTokens(env);
    if (!tokens) throw new Error("No tokens stored. Visit /auth to connect Google.");
    tokens = await refreshAccessTokenIfNeeded(env, tokens);
    const accessToken = tokens && tokens.access_token;
    if (!accessToken) {
        throw new Error("No access token available. Visit /auth again.");
    }
    return accessToken;
}

export async function callBusinessProfileAPI(env, method, url, body) {
    const accessToken = await getAccessToken(env);
    const init = {
        method,
        headers: {
            Authorization: "Bearer " + accessToken,
            "Content-Type": "application/json"
        }
    };
    if (body !== undefined && body !== null) {
        init.body = JSON.stringify(body);
    }

    const resp = await fetch(url, init);
    if (!resp.ok) {
        const text = await resp.text();
        throw new Error(text || ("Google API error " + resp.status));
    }
    const data = await resp.json();
    return { data };
}

// Simple helper variant that mirrors the fetch API signature
export async function callBusinessProfileApi(env, url, init = {}) {
    const accessToken = await getAccessToken(env);

    const headers = new Headers(init.headers || {});
    headers.set("Authorization", `Bearer ${accessToken}`);
    if (!headers.has("Content-Type") && init.body) {
        headers.set("Content-Type", "application/json");
    }

    const resp = await fetch(url, {...init, headers });
    if (!resp.ok) {
        const text = await resp.text();
        throw new Error(`GBP API ${resp.status} ${resp.statusText}: ${text}`);
    }
    return resp.json();
}