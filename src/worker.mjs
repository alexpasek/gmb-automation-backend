import { getJson, setJson } from "./storage.mjs";
import { buildAuthUrl, exchangeCodeForTokens, callBusinessProfileApi } from "./google.mjs";
import {
    getProfiles,
    saveProfiles,
    getPostsHistory,
    getSchedulerConfig,
    setSchedulerConfig,
    getSchedulerStatus,
    runSchedulerOnce,
    runSchedulerNow,
    scheduledTick,
    appendPhotosToProfile,
    enqueueScheduledPost,
    enqueueScheduledBulk,
    draftScheduledBulk,
    updateScheduledPost,
    getScheduledPosts,
    deleteScheduledPost,
    commitScheduledPosts,
    getScheduledPhotos,
    enqueueScheduledPhoto,
    saveScheduledPhotos,
    deletePhotoScheduled,
    getCycleStateForProfile,
    getAllScheduledPhotos,
    uploadPhotoToGmb,
    fetchLatestMedia,
    fetchMediaPaged
} from "./gmb.mjs";
import { aiGenerateSummaryAndHashtags, pickNeighbourhood } from "./ai.mjs";

const VERSION = "1.0.0";

const CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,POST,PUT,PATCH,DELETE,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization"
};

function jsonResponse(obj, status = 200) {
    return new Response(JSON.stringify(obj), {
        status,
        headers: {
            ...CORS_HEADERS,
            "Content-Type": "application/json"
        }
    });
}

function textResponse(text, status = 200, extra = {}) {
    return new Response(text, {
        status,
        headers: {
            ...CORS_HEADERS,
            ...extra
        }
    });
}

function optionsResponse() {
    return new Response(null, {
        status: 204,
        headers: CORS_HEADERS
    });
}

function decodeBase64ToArrayBuffer(base64) {
    const binary = atob(base64);
    const len = binary.length;
    const bytes = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
        bytes[i] = binary.charCodeAt(i);
    }
    return bytes.buffer;
}

// ---- D1 helpers for profiles ----
async function ensureKv(env) {
    await env.D1_DB.prepare(`
    CREATE TABLE IF NOT EXISTS kv (
      key TEXT PRIMARY KEY,
      value TEXT,
      updated_at TEXT NOT NULL
    );
  `).run();
}

async function getProfilesFromDb(env) {
    await ensureKv(env);
    const row = await env.D1_DB.prepare(
            "SELECT value FROM kv WHERE key = ?"
        )
        .bind("profiles")
        .first();

    if (!row || !row.value) return [];
    try {
        const parsed = JSON.parse(row.value);
        return Array.isArray(parsed) ? parsed : [];
    } catch {
        return [];
    }
}

async function saveProfilesToDb(env, profiles) {
    await ensureKv(env);
    const json = JSON.stringify(profiles);
    const now = new Date().toISOString();
    await env.D1_DB.prepare(
            `
    INSERT INTO kv (key, value, updated_at)
    VALUES (?1, ?2, ?3)
    ON CONFLICT(key) DO UPDATE SET
      value = excluded.value,
      updated_at = excluded.updated_at
    `
        )
        .bind("profiles", json, now)
        .run();
}

// "accounts/123/locations/456" -> "456"
function extractLocationId(name) {
    if (!name) return "";
    const parts = String(name).split("/");
    return parts[parts.length - 1] || "";
}

async function parseJsonBody(request) {
    try {
        return await request.json();
    } catch {
        return {};
    }
}

// Upload helper: save to R2 (MEDIA_BUCKET) and return public URL
async function handleUpload(request, env) {
    try {
        const contentType = request.headers.get("Content-Type") || "";
        if (!contentType.toLowerCase().includes("multipart/form-data")) {
            return jsonResponse({ error: "Expected multipart/form-data" }, 400);
        }

        const formData = await request.formData();
        const rawFolder = String(formData.get("folder") || "").trim();
        const folder = rawFolder
            .replace(/[^a-zA-Z0-9/_-]+/g, "-") // keep simple folder charset
            .replace(/\/+/g, "/")
            .replace(/^\/+|\/+$/g, "")
            .replace(/(\.\.|\.)/g, "");

        const files = [
            ...formData.getAll("file"),
            ...formData.getAll("photo"),
            ...formData.getAll("image"),
        ].filter((f) => f && typeof f !== "string");

        if (!files.length) {
            return jsonResponse({ error: "No file field found" }, 400);
        }

        const origin = new URL(request.url).origin;
        const uploaded = [];
        const failed = [];
        const maxBytes = 20 * 1024 * 1024; // per-file limit

        for (const file of files) {
            try {
                const arrayBuffer = await file.arrayBuffer();
                const size = arrayBuffer.byteLength;
                if (size > maxBytes) {
                    failed.push({ name: file.name || "file", error: `Too large (${(size / 1024 / 1024).toFixed(1)}MB, max 20MB)` });
                    continue;
                }

                let ext = ".jpg";
                if (file.name && /\.[a-zA-Z0-9]+$/.test(file.name)) {
                    ext = file.name.match(/\.[a-zA-Z0-9]+$/)[0].toLowerCase();
                }

                const prefix = folder ? `gmb/${folder}/` : "gmb/";
                const key =
                    prefix +
                    Date.now() +
                    "-" +
                    Math.random().toString(36).slice(2) +
                    ext;

                const ct = file.type || guessContentTypeFromExt(ext) || "image/jpeg";
                await env.MEDIA_BUCKET.put(key, arrayBuffer, {
                    httpMetadata: { contentType: ct }
                });
                const publicUrl = origin + "/media/" + encodeURIComponent(key);
                uploaded.push(publicUrl);
            } catch (err) {
                failed.push({ name: file.name || "file", error: String(err && err.message ? err.message : err) });
            }
        }

        if (!uploaded.length && failed.length) {
            return jsonResponse({ error: "All uploads failed", failed }, 400);
        }

        return jsonResponse({
            url: uploaded[0] || "",
            uploaded,
            failed
        }, 200);
    } catch (e) {
        console.error("Upload error:", e);
        return jsonResponse({ error: "Upload failed" }, 500);
    }
}

function guessContentTypeFromExt(ext) {
    switch ((ext || "").toLowerCase()) {
        case ".png":
            return "image/png";
        case ".webp":
            return "image/webp";
        case ".jpg":
        case ".jpeg":
            return "image/jpeg";
        default:
            return "image/jpeg";
    }
}

async function serveMediaFromR2(key, env) {
    if (!key) {
        return new Response("Missing key", { status: 400 });
    }

    const obj = await env.MEDIA_BUCKET.get(key);
    if (!obj) {
        return new Response("Not found", { status: 404 });
    }

    const headers = new Headers();
    if (obj.httpMetadata && obj.httpMetadata.contentType) {
        headers.set("Content-Type", obj.httpMetadata.contentType);
    } else {
        headers.set("Content-Type", "application/octet-stream");
    }
    headers.set("Cache-Control", "public, max-age=31536000, immutable");
    headers.set("Access-Control-Allow-Origin", "*");

    return new Response(obj.body, { status: 200, headers });
}

async function handleRequest(request, env, ctx) {
    const url = new URL(request.url);
    const { pathname, searchParams } = url;

    if (request.method === "OPTIONS") {
        return optionsResponse();
    }

    if (pathname === "/debug-env" && request.method === "GET") {
        const body = JSON.stringify({
                GOOGLE_CLIENT_ID: env.GOOGLE_CLIENT_ID,
                GOOGLE_REDIRECT_URI: env.GOOGLE_REDIRECT_URI,
                HAS_GOOGLE_CLIENT_SECRET: !!env.GOOGLE_CLIENT_SECRET
            },
            null,
            2
        );
        return new Response(body, {
            status: 200,
            headers: { "Content-Type": "application/json" }
        });
    }

    if (pathname === "/health") {
        return jsonResponse({ ok: true, status: "healthy" });
    }

    if (pathname === "/version") {
        return jsonResponse({ name: "gmb-automation-backend", version: VERSION });
    }

    // --- GBP: list accounts ---
    if (pathname === "/accounts" && request.method === "GET") {
        const url =
            "https://mybusinessbusinessinformation.googleapis.com/v1/accounts";
        const data = await callBusinessProfileApi(env, url);
        return jsonResponse(data);
    }

    // --- GBP: list locations for a given account ---
    if (pathname === "/locations" && request.method === "GET") {
        const accountId = searchParams.get("accountId") || env.GBP_ACCOUNT_ID;
        if (!accountId) {
            return jsonResponse({ error: "Missing accountId" }, 400);
        }

        const readMask =
            searchParams.get("readMask") ||
            "name,title,storeCode,websiteUri,phoneNumbers,metadata,storefrontAddress";

        let pageToken = searchParams.get("pageToken") || "";
        const locations = [];

        do {
            let url =
                `https://mybusinessbusinessinformation.googleapis.com/v1/accounts/${accountId}/locations` +
                `?readMask=${encodeURIComponent(readMask)}&pageSize=100`;
            if (pageToken) {
                url += `&pageToken=${encodeURIComponent(pageToken)}`;
            }

            const data = await callBusinessProfileApi(env, url);
            if (Array.isArray(data.locations)) {
                locations.push(...data.locations);
            }
            pageToken = data.nextPageToken || "";
        } while (pageToken);

        return jsonResponse({ accountId, locations });
    }

    if ((pathname === "/auth" || pathname === "/auth/") && request.method === "GET") {
        const authUrl = buildAuthUrl(env);

        // Debug logging to verify env and redirect URL during OAuth starts
        console.log("AUTH DEBUG: env.GOOGLE_CLIENT_ID =", env.GOOGLE_CLIENT_ID);
        console.log("AUTH DEBUG: env.GOOGLE_REDIRECT_URI =", env.GOOGLE_REDIRECT_URI);
        console.log("AUTH DEBUG: redirecting to:", authUrl);

        return Response.redirect(authUrl, 302);
    }

    if (
        (pathname === "/oauth2callback" || pathname === "/oauth2callback/") &&
        request.method === "GET"
    ) {
        // 🔍 DEBUG: log full URL and all query params
        console.log("CALLBACK DEBUG: request.url =", request.url);
        const allParams = [];
        for (const [k, v] of searchParams.entries()) {
            allParams.push(`${k}=${v}`);
        }
        console.log("CALLBACK DEBUG: searchParams =", allParams.join("&") || "(none)");

        const errorParam = searchParams.get("error");
        if (errorParam) {
            return textResponse(
                "Google OAuth error from Google: " + errorParam +
                ". Go back to /auth and try again.",
                400, { "Content-Type": "text/plain" }
            );
        }

        const code = searchParams.get("code");
        if (!code) {
            return textResponse(
                "Missing code. This page should only be opened by Google after you start from /auth.",
                400, { "Content-Type": "text/plain" }
            );
        }

        console.log("CALLBACK DEBUG: have code starting with", code.slice(0, 8), "...");

        await exchangeCodeForTokens(env, code);
        return textResponse(
            "Google Business Profile is connected. You can close this tab.",
            200, { "Content-Type": "text/plain" }
        );
    }

    // Sync profiles from Google locations into D1 (kv key = "profiles")
    if (pathname === "/profiles/sync-from-google" && request.method === "POST") {
        const body = await request.json().catch(() => ({}));

        // use body.accountId if given, otherwise env.GBP_ACCOUNT_ID
        const accountId = body.accountId || env.GBP_ACCOUNT_ID;
        if (!accountId) {
            return jsonResponse({ error: "Missing accountId" }, 400);
        }

        // 1) Load existing profiles from DB so we can preserve neighbourhoods, keywords, etc.
        const existing = await getProfilesFromDb(env);
        const byLocationId = new Map();
        for (const p of existing) {
            if (p && p.locationId) {
                byLocationId.set(String(p.locationId), p);
            }
        }

        // 2) Fetch ALL locations for this account (same logic as /locations)
        const readMask =
            "name,title,storeCode,websiteUri,phoneNumbers,metadata,storefrontAddress";
        let pageToken = "";
        const locations = [];

        do {
            let url =
                `https://mybusinessbusinessinformation.googleapis.com/v1/accounts/${accountId}/locations` +
                `?readMask=${encodeURIComponent(readMask)}&pageSize=100`;
            if (pageToken) {
                url += `&pageToken=${encodeURIComponent(pageToken)}`;
            }

            const data = await callBusinessProfileApi(env, url); // you already use this in /locations
            if (Array.isArray(data.locations)) {
                locations.push(...data.locations);
            }
            pageToken = data.nextPageToken || "";
        } while (pageToken);

        // 3) Build merged profile list
        const merged = [];

        for (const loc of locations) {
            const locId = extractLocationId(loc.name);
            const existingProfile = byLocationId.get(locId) || {};

            const addr = loc.storefrontAddress || {};
            const city =
                addr.locality ||
                addr.postalCode ||
                existingProfile.city ||
                "";

            const businessName =
                loc.title ||
                existingProfile.businessName ||
                `Location ${locId}`;

            const websiteUri =
                loc.websiteUri ||
                existingProfile.landingUrl ||
                (existingProfile.defaults && existingProfile.defaults.linkUrl) ||
                "";

            const primaryPhone =
                (loc.phoneNumbers && loc.phoneNumbers.primaryPhone) ||
                existingProfile.phone ||
                "";

            const metadata = loc.metadata || {};

            const profile = {
                // IDs
                profileId: existingProfile.profileId ||
                    (loc.storeCode ?
                        `profile-${loc.storeCode}` :
                        `profile-${accountId}-${locId}`),
                accountId: accountId,
                locationId: locId,

                // Business info
                businessName,
                city,
                storeCode: loc.storeCode || existingProfile.storeCode || "",

                // Links / phone
                landingUrl: websiteUri,
                phone: primaryPhone,

                // Keep previous custom fields if they exist
                neighbourhoods: existingProfile.neighbourhoods || [],
                keywords: existingProfile.keywords || [],
                photoPool: existingProfile.photoPool || [],
                defaults: existingProfile.defaults || {},

                disabled: typeof existingProfile.disabled === "boolean" ?
                    existingProfile.disabled : false,

                // Extra GBP metadata (handy for links in posts)
                mapsUri: metadata.mapsUri || existingProfile.mapsUri || "",
                placeReviewUri: metadata.placeReviewUri || existingProfile.placeReviewUri || ""
            };

            merged.push(profile);
        }

        // 4) Save to D1
        await saveProfilesToDb(env, merged);

        return jsonResponse({
            ok: true,
            accountId,
            locations: locations.length,
            profiles: merged.length
        });
    }

    if (pathname === "/profiles" && request.method === "GET") {
        const profiles = await getProfilesFromDb(env);
        return jsonResponse({ profiles });
    }

    if (pathname === "/profiles" && request.method === "PUT") {
        const body = await parseJsonBody(request);
        const list = Array.isArray(body.profiles) ? body.profiles : [];
        await saveProfiles(env, list);
        const profiles = await getProfiles(env);
        return jsonResponse({ ok: true, profiles });
    }

    // PATCH /profiles/:id/defaults
    let m = pathname.match(/^\/profiles\/([^/]+)\/defaults$/);
    if (m && request.method === "PATCH") {
        const id = decodeURIComponent(m[1]);
        const body = await parseJsonBody(request);
        const profiles = await getProfiles(env);
        const idx = profiles.findIndex((p) => p && p.profileId === id);
        if (idx === -1) return jsonResponse({ error: "Profile not found" }, 404);
        const target = profiles[idx];
        const defaults = {...(target.defaults || {}) };

        if (body.hasOwnProperty("cta")) defaults.cta = body.cta;
        if (body.hasOwnProperty("linkUrl")) defaults.linkUrl = body.linkUrl;
        if (body.hasOwnProperty("mediaUrl")) defaults.mediaUrl = body.mediaUrl;
        if (body.hasOwnProperty("phone")) defaults.phone = body.phone;
        if (body.hasOwnProperty("linkOptions")) {
            const opts = Array.isArray(body.linkOptions) ? body.linkOptions : [];
            defaults.linkOptions = opts
                .map((u) => String(u || "").trim())
                .filter(Boolean);
        }
        if (body.hasOwnProperty("reviewLink")) defaults.reviewLink = String(body.reviewLink || "").trim();
        if (body.hasOwnProperty("serviceAreaLink"))
            defaults.serviceAreaLink = String(body.serviceAreaLink || "").trim();
        if (body.hasOwnProperty("areaMapLink")) defaults.areaMapLink = String(body.areaMapLink || "").trim();
        if (body.hasOwnProperty("photoLat")) defaults.photoLat = String(body.photoLat || "").trim();
        if (body.hasOwnProperty("photoLng")) defaults.photoLng = String(body.photoLng || "").trim();
        if (body.hasOwnProperty("photoCityOverride"))
            defaults.photoCityOverride = String(body.photoCityOverride || "").trim();
        if (body.hasOwnProperty("photoNeighbourhood"))
            defaults.photoNeighbourhood = String(body.photoNeighbourhood || "").trim();
        if (body.hasOwnProperty("photoNeighbourhoods")) {
            const list = Array.isArray(body.photoNeighbourhoods) ?
                body.photoNeighbourhoods :
                String(body.photoNeighbourhoods || "")
                .split(/\r?\n|,/)
                .map((s) => s.trim())
                .filter(Boolean);
            defaults.photoNeighbourhoods = list;
        }
        if (body.hasOwnProperty("photoRandomizeCoords"))
            defaults.photoRandomizeCoords = !!body.photoRandomizeCoords;
        if (body.hasOwnProperty("photoRandomizeRadius")) {
            const radius = Number(body.photoRandomizeRadius);
            if (!isNaN(radius)) defaults.photoRandomizeRadius = radius;
        }
        if (body.hasOwnProperty("photoKeywords")) defaults.photoKeywords = String(body.photoKeywords || "").trim();
        if (body.hasOwnProperty("photoCategories")) defaults.photoCategories = String(body.photoCategories || "").trim();
        if (body.hasOwnProperty("disabled")) target.disabled = !!body.disabled;

        target.defaults = defaults;
        profiles[idx] = target;
        await saveProfiles(env, profiles);
        return jsonResponse({ ok: true, profile: target });
    }

    // POST /profiles/:id/bulk-access
    m = pathname.match(/^\/profiles\/([^/]+)\/bulk-access$/);
    if (m && request.method === "POST") {
        const id = decodeURIComponent(m[1]);
        const body = await parseJsonBody(request);
        const profiles = await getProfiles(env);
        const idx = profiles.findIndex((p) => p && p.profileId === id);
        if (idx === -1) return jsonResponse({ error: "Profile not found" }, 404);
        const target = profiles[idx];
        target.disabled = body.enabled === false ? true : false;
        profiles[idx] = target;
        await saveProfiles(env, profiles);
        return jsonResponse({ ok: true, profile: target });
    }

    // POST /profiles/:id/photos   { urls: [...] }
    m = pathname.match(/^\/profiles\/([^/]+)\/photos$/);
    if (m && request.method === "POST") {
        const id = decodeURIComponent(m[1]);
        const body = await parseJsonBody(request);
        const urls = Array.isArray(body.urls) ? body.urls : [];
        const items = Array.isArray(body.items) ? body.items : urls;
        try {
            const updated = await appendPhotosToProfile(env, id, items);
            return jsonResponse({ ok: true, profile: updated });
        } catch (err) {
            return jsonResponse({ error: err.message || "Failed to append photos" }, 400);
        }
    }

    // POST /ai/captions
    if (pathname === "/ai/captions" && request.method === "POST") {
        const body = await parseJsonBody(request);
        const profileId = body.profileId || "";
        const serviceType = body.serviceType || "";
        const count = Math.max(1, Math.min(5, parseInt(body.count, 10) || 3));
        if (!profileId) return jsonResponse({ error: "Missing profileId" }, 400);
        const profiles = await getProfiles(env);
        const profile = profiles.find((p) => p.profileId === profileId);
        if (!profile) return jsonResponse({ error: "Profile not found" }, 404);

        const caps = [];
        for (let i = 0; i < count; i++) {
            const neighbourhood = pickNeighbourhood(profile);
            const gen = await aiGenerateSummaryAndHashtags(env, profile, neighbourhood);
            let text = (gen.summary || "").trim();
            if (gen.hashtags && gen.hashtags.length) {
                text += "\n\n" + gen.hashtags.join(" ");
            }
            if (serviceType) {
                text = `${serviceType} · ${text}`;
            }
            caps.push(text.trim());
        }
        return jsonResponse({ captions: caps });
    }

    if (pathname === "/generate-post-by-profile" && request.method === "GET") {
        const profileId = searchParams.get("profileId");
        if (!profileId) return jsonResponse({ error: "Missing profileId" }, 400);
        const profiles = await getProfiles(env);
        const profile = profiles.find((p) => p && p.profileId === profileId);
        if (!profile) return jsonResponse({ error: "Profile not found" }, 404);

        const defaults = profile.defaults || {};
        const neighbourhood = pickNeighbourhood(profile);
        const gen = await aiGenerateSummaryAndHashtags(env, profile, neighbourhood);
        const quickLinks = [
                { label: "Reviews ►", url: defaults.reviewLink },
                { label: "Service Area ►", url: defaults.serviceAreaLink },
                { label: "Area Map ►", url: defaults.areaMapLink },
            ]
            .map((q) => {
                const val = String(q.url || "").trim();
                return /^https?:\/\//i.test(val) ? `${q.label} ${val}` : "";
            })
            .filter(Boolean);
        let post =
            (gen.summary || "") +
            (gen.hashtags && gen.hashtags.length ?
                "\n\n" + gen.hashtags.join(" ") :
                "");
        if (quickLinks.length) {
            post += "\n\n" + quickLinks.join("\n");
        }
        return jsonResponse({
            profileId,
            businessName: profile.businessName || "",
            city: profile.city || "",
            neighbourhood,
            post
        });
    }

    if (pathname === "/post-now" && request.method === "POST") {
        const body = await parseJsonBody(request);
        const { profileId } = body;
        if (!profileId) return jsonResponse({ error: "Missing profileId" }, 400);
        const { postToGmb } = await
        import ("./gmb.mjs");
        const result = await postToGmb(env, body);
        return jsonResponse({ ok: true, result });
    }

    if (pathname === "/scheduled-posts" && request.method === "GET") {
        const items = await getScheduledPosts(env);
        return jsonResponse({ items });
    }

    if (pathname === "/scheduled-posts" && request.method === "POST") {
        const body = await parseJsonBody(request);
        const runAt = body.runAt ? new Date(body.runAt) : null;
        if (!runAt || isNaN(runAt.getTime())) {
            return jsonResponse({ error: "Invalid runAt" }, 400);
        }
        const profileId = body.profileId || "";
        if (!profileId) return jsonResponse({ error: "Missing profileId" }, 400);
        const item = await enqueueScheduledPost(env, {
            profileId,
            runAt: runAt.toISOString(),
            body: body.body || {}
        });
        return jsonResponse({ ok: true, item });
    }

    if (pathname === "/scheduled-posts/bulk" && request.method === "POST") {
        const body = await parseJsonBody(request);
        try {
            const res = await enqueueScheduledBulk(env, body || {});
            return jsonResponse({ ok: true, ...res });
        } catch (e) {
            return jsonResponse({ error: e.message || "Bulk schedule failed" }, 400);
        }
    }

    // Photo scheduler endpoints
    if (pathname === "/photo-scheduled" && request.method === "GET") {
        const urlObj = new URL(request.url);
        const includeAll = urlObj.searchParams.get("all") === "1";
        const items = includeAll ? await getAllScheduledPhotos(env) : await getScheduledPhotos(env);
        return jsonResponse({ items });
    }

    if (pathname === "/photo-latest" && request.method === "GET") {
        const urlObj = new URL(request.url);
        const profileId = urlObj.searchParams.get("profileId") || "";
        const limitRaw = urlObj.searchParams.get("limit") || "";
        const limit = Math.max(1, Math.min(50, Number(limitRaw) || 10));
        try {
            const items = await fetchLatestMedia(env, profileId, limit);
            return jsonResponse({ items });
        } catch (e) {
            return jsonResponse({ error: e.message || "Failed to fetch media" }, 400);
        }
    }

    if (pathname === "/photo-latest-debug" && request.method === "GET") {
        const urlObj = new URL(request.url);
        const profileId = urlObj.searchParams.get("profileId") || "";
        const limitRaw = urlObj.searchParams.get("limit") || "20";
        const pagesRaw = urlObj.searchParams.get("pages") || "3";
        const limit = Math.max(1, Math.min(50, Number(limitRaw) || 20));
        const pages = Math.max(1, Math.min(10, Number(pagesRaw) || 3));
        try {
            const res = await fetchMediaPaged(env, profileId, limit, pages);
            return jsonResponse(res);
        } catch (e) {
            return jsonResponse({ error: e.message || "Failed to fetch media" }, 400);
        }
    }

    if (pathname === "/photo-now" && request.method === "POST") {
        const body = await parseJsonBody(request);
        const profileId = String(body.profileId || "").trim();
        const mediaUrl = String(body.mediaUrl || "").trim();
        if (!profileId || !mediaUrl) {
            return jsonResponse({ error: "Missing profileId or mediaUrl" }, 400);
        }
        const profiles = await getProfiles(env);
        const profile = profiles.find((p) => p && p.profileId === profileId);
        if (!profile) return jsonResponse({ error: "Profile not found" }, 404);
        try {
            await uploadPhotoToGmb(env, profile, { mediaUrl, caption: body.caption || "" });
            return jsonResponse({ ok: true });
        } catch (e) {
            return jsonResponse({ error: e.message || "Photo post failed" }, 400);
        }
    }

    if (pathname === "/photo-scheduled" && request.method === "POST") {
        const body = await parseJsonBody(request);
        try {
            const item = await enqueueScheduledPhoto(env, body || {});
            return jsonResponse({ ok: true, item });
        } catch (e) {
            return jsonResponse({ error: e.message || "Failed to schedule photo" }, 400);
        }
    }

    if (pathname === "/photo-scheduled/bulk" && request.method === "POST") {
        const body = await parseJsonBody(request);
        try {
            const list = Array.isArray(body.items) ? body.items : [];
            await saveScheduledPhotos(env, list);
            return jsonResponse({ ok: true, count: list.length });
        } catch (e) {
            return jsonResponse({ error: e.message || "Failed to save photo schedules" }, 400);
        }
    }

    if (pathname.startsWith("/photo-scheduled/") && request.method === "DELETE") {
        const id = decodeURIComponent(pathname.split("/").pop() || "");
        try {
            await deletePhotoScheduled(env, id);
            return jsonResponse({ ok: true });
        } catch (e) {
            return jsonResponse({ error: e.message || "Delete failed" }, 400);
        }
    }

    if (pathname === "/scheduled-posts/draft" && request.method === "POST") {
        const body = await parseJsonBody(request);
        try {
            const drafts = await draftScheduledBulk(env, body || {});
            return jsonResponse({ ok: true, items: drafts });
        } catch (e) {
            return jsonResponse({ error: e.message || "Draft failed" }, 400);
        }
    }

    if (pathname === "/scheduled-posts/commit" && request.method === "POST") {
        const body = await parseJsonBody(request);
        const items = Array.isArray(body.items) ? body.items : [];
        try {
            const count = await commitScheduledPosts(env, items);
            return jsonResponse({ ok: true, count });
        } catch (e) {
            return jsonResponse({ error: e.message || "Commit failed" }, 400);
        }
    }

    if (pathname.startsWith("/scheduled-posts/") && request.method === "DELETE") {
        const id = decodeURIComponent(pathname.split("/").pop() || "");
        try {
            await deleteScheduledPost(env, id);
            return jsonResponse({ ok: true });
        } catch (e) {
            return jsonResponse({ error: e.message || "Delete failed" }, 400);
        }
    }

    if (pathname.startsWith("/scheduled-posts/") && request.method === "PUT") {
        const id = decodeURIComponent(pathname.split("/").pop() || "");
        const body = await parseJsonBody(request);
        try {
            const updated = await updateScheduledPost(env, id, body || {});
            return jsonResponse({ ok: true, item: updated });
        } catch (e) {
            return jsonResponse({ error: e.message || "Update failed" }, 400);
        }
    }

    if (pathname === "/post-now-all" && request.method === "POST") {
        const profiles = await getProfiles(env);
        const { postToGmb } = await
        import ("./gmb.mjs");
        const results = [];
        for (const p of profiles) {
            if (!p || p.disabled) continue;
            try {
                const r = await postToGmb(env, { profileId: p.profileId });
                results.push({ profileId: p.profileId, ok: true, data: r.data });
            } catch (e) {
                results.push({
                    profileId: p.profileId,
                    ok: false,
                    error: String(e && e.message ? e.message : e)
                });
            }
        }
        return jsonResponse({ ok: true, results });
    }

    if (pathname === "/scheduler/config" && request.method === "GET") {
        const cfg = await getSchedulerConfig(env);
        return jsonResponse(cfg);
    }

    if (pathname === "/scheduler/config" && request.method === "PUT") {
        const body = await parseJsonBody(request);
        const cfg = await setSchedulerConfig(env, body || {});
        return jsonResponse({ ok: true, config: cfg });
    }

    if (pathname === "/scheduler/status" && request.method === "GET") {
        const status = await getSchedulerStatus(env);
        return jsonResponse(status);
    }

    if (pathname === "/cycle-state" && request.method === "GET") {
        const profileId = searchParams.get("profileId") || "";
        try {
            const state = await getCycleStateForProfile(env, profileId);
            if (profileId) {
                return jsonResponse({ profileId, state });
            }
            return jsonResponse({ state });
        } catch (e) {
            return jsonResponse({ error: e.message || "Failed to load cycle state" }, 400);
        }
    }

    if (pathname === "/scheduler/run-once" && request.method === "POST") {
        const result = await runSchedulerOnce(env);
        return jsonResponse(result);
    }

    if (pathname === "/upload" && request.method === "POST") {
        return handleUpload(request, env);
    }

    if (pathname === "/uploads-list" && request.method === "GET") {
        try {
            const list = await env.MEDIA_BUCKET.list({ prefix: "", limit: 500 });
            const files = (list?.objects || []).map((obj) => obj.key);
            const folderCounts = new Map();
            for (const k of files) {
                const clean = String(k || "").replace(/^gmb\//, "");
                const idx = clean.lastIndexOf("/");
                const folder = idx === -1 ? "" : clean.slice(0, idx);
                folderCounts.set(folder, (folderCounts.get(folder) || 0) + 1);
            }
            if (!folderCounts.has("")) folderCounts.set("", 0);
            const folders = Array.from(folderCounts.entries()).map(
                ([name, count]) => ({ name, count })
            );
            const origin = new URL(request.url).origin.replace(/\/+$/, "");
            const urls = files.map((k) => `${origin}/media/${encodeURIComponent(k)}`);
            return jsonResponse({ count: files.length, files, urls, folders });
        } catch (err) {
            console.error("uploads-list error", err);
            return jsonResponse({ error: "Failed to list uploads" }, 500);
        }
    }

    // DELETE /uploads/:key
    const delMatch = pathname.match(/^\/uploads\/(.+)$/);
    if (delMatch && request.method === "DELETE") {
        const rawKey = decodeURIComponent(delMatch[1]);
        const key = rawKey.replace(/^media\//, "").replace(/^\/+/, "");
        if (!key) return jsonResponse({ error: "Missing key" }, 400);
        try {
            await env.MEDIA_BUCKET.delete(key);
            return jsonResponse({ ok: true, deleted: key });
        } catch (err) {
            console.error("delete upload error", err);
            return jsonResponse({ error: "Failed to delete" }, 500);
        }
    }

    if (pathname === "/uploads-check" && request.method === "GET") {
        const origin = new URL(request.url).origin.replace(/\/+$/, "");
        return jsonResponse({ ok: true, url: origin + "/media", status: 200 });
    }

    // AI image generator -> upload to R2 -> return /media URL
    if (pathname === "/ai/image" && request.method === "POST") {
        if (!env.OPENAI_API_KEY) {
            return jsonResponse({ error: "OPENAI_API_KEY not set" }, 500);
        }
        const body = await parseJsonBody(request);
        const prompt = (body.prompt || "").trim() || "home renovation photo";
        try {
            // Use DALL·E 3 with URL response, then fetch the image and store in R2
            const openaiResp = await fetch("https://api.openai.com/v1/images/generations", {
                method: "POST",
                headers: {
                    Authorization: `Bearer ${env.OPENAI_API_KEY}`,
                    "Content-Type": "application/json"
                },
                body: JSON.stringify({
                    model: "dall-e-3",
                    prompt,
                    size: "1024x1024",
                    quality: "standard",
                    response_format: "url"
                })
            });
            if (!openaiResp.ok) {
                const errText = await openaiResp.text().catch(() => "");
                return jsonResponse({ error: "OpenAI error: " + errText }, 500);
            }
            const data = await openaiResp.json();
            const imgUrl = data?.data?.[0]?.url;
            if (!imgUrl) return jsonResponse({ error: "No image returned" }, 500);

            const imgResp = await fetch(imgUrl);
            if (!imgResp.ok) {
                return jsonResponse({ error: "Failed to fetch generated image" }, 500);
            }
            const arrayBuf = await imgResp.arrayBuffer();
            const key =
                "ai/" +
                Date.now() +
                "-" +
                Math.random().toString(36).slice(2) +
                ".jpg";
            await env.MEDIA_BUCKET.put(key, arrayBuf, {
                httpMetadata: { contentType: "image/jpeg" }
            });

            const origin = new URL(request.url).origin.replace(/\/+$/, "");
            const url = origin + "/media/" + encodeURIComponent(key);
            return jsonResponse({ url, key, prompt });
        } catch (err) {
            console.error("AI image error", err);
            return jsonResponse({ error: err.message || "AI image failed" }, 500);
        }
    }

    if (pathname.startsWith("/media/") && request.method === "GET") {
        const key = decodeURIComponent(pathname.slice("/media/".length));
        return serveMediaFromR2(key, env);
    }

    m = pathname.match(/^\/scheduler\/run-now\/([^/]+)$/);
    if (m && request.method === "POST") {
        const id = decodeURIComponent(m[1]);
        const result = await runSchedulerNow(env, id);
        return jsonResponse({ ok: true, result });
    }

    if (pathname === "/posts/history" && request.method === "GET") {
        const profileId = searchParams.get("profileId");
        const limitRaw = searchParams.get("limit");
        const limit = limitRaw ? parseInt(limitRaw, 10) || 50 : 50;
        const items = await getPostsHistory(env, profileId || null, limit);
        return jsonResponse({ items });
    }

    return jsonResponse({ error: "Not found" }, 404);
}

export default {
    async fetch(request, env, ctx) {
        try {
            return await handleRequest(request, env, ctx);
        } catch (e) {
            console.error("Unhandled error:", e);
            return jsonResponse({ error: String(e && e.message ? e.message : e) },
                500
            );
        }
    },

    async scheduled(controller, env, ctx) {
        try {
            await scheduledTick(env);
        } catch (e) {
            console.error("Scheduled error:", e);
        }
    }
};
