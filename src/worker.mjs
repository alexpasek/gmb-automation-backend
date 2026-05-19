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
    runDueScheduledPhotos,
    getCycleStateForProfile,
    getAllScheduledPhotos,
    uploadPhotoToGmb,
    fetchLatestMedia,
    fetchMediaPaged,
    fetchPerformanceMetrics,
    ensureAbsoluteMediaUrl,
    buildQuickLinkLines,
    insertQuickLinksBeforeHashtags,
    composeAiTemplatePost,
    fetchLocationBasics
} from "./gmb.mjs";
import { aiGenerateSummaryAndHashtags, pickNeighbourhood, safeJoinHashtags } from "./ai.mjs";

const VERSION = "1.0.0";

const CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,POST,PUT,PATCH,DELETE,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, x-agent-api-key"
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

function privacyPolicyHtml() {
    return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>GMB Automation Privacy Policy</title>
  <style>
    body { font-family: Arial, sans-serif; line-height: 1.6; max-width: 820px; margin: 40px auto; padding: 0 20px; color: #172033; }
    h1, h2 { line-height: 1.25; }
    .muted { color: #5d6678; }
  </style>
</head>
<body>
  <h1>GMB Automation Privacy Policy</h1>
  <p class="muted">Last updated: May 19, 2026</p>
  <p>GMB Automation is a private workflow tool used to create, upload, schedule, and publish Google Business Profile content.</p>

  <h2>Information Processed</h2>
  <p>The service may process Google Business Profile profile details, post text, scheduling settings, image URLs, uploaded image files, and generated AI image metadata needed to perform requested posting workflows.</p>

  <h2>How Information Is Used</h2>
  <p>Information is used only to operate the requested automation features, including listing profiles, saving images to the media gallery, generating AI images, scheduling posts, and publishing posts or photos to Google Business Profile.</p>

  <h2>Storage</h2>
  <p>Profile configuration and scheduling data may be stored in Cloudflare D1. Uploaded and generated images may be stored in Cloudflare R2. API keys and OAuth tokens are stored as Cloudflare Worker secrets or backend storage needed for service operation.</p>

  <h2>Sharing</h2>
  <p>Data is sent only to service providers required for the workflow, including Cloudflare, Google Business Profile APIs, and OpenAI image/text generation APIs. Data is not sold.</p>

  <h2>Retention</h2>
  <p>Stored profiles, schedules, post history, and media remain until deleted through the application or backend storage controls.</p>

  <h2>Contact</h2>
  <p>For privacy questions, contact: webtoronto22@gmail.com</p>
</body>
</html>`;
}

function getAgentOpenApiSchema(request) {
    const origin = new URL(request.url).origin.replace(/\/+$/, "");
    const targetProfileProperties = {
        profileId: { type: "string" },
        profileQuery: { type: "string" },
        businessName: { type: "string" },
        city: { type: "string" },
        query: { type: "string" }
    };
    const objectSchema = (properties, required = []) => ({
        type: "object",
        properties: {
            ...targetProfileProperties,
            ...properties
        },
        required
    });
    return {
        openapi: "3.1.0",
        info: {
            title: "GMB Automation Agent API",
            version: "1.0.0"
        },
        servers: [{ url: origin }],
        components: {
            securitySchemes: {
                AgentApiKey: {
                    type: "apiKey",
                    in: "header",
                    name: "x-agent-api-key"
                }
            },
            schemas: {}
        },
        security: [{ AgentApiKey: [] }],
        paths: {
            "/agent/profiles": {
                get: {
                    operationId: "listProfiles",
                    summary: "List Google Business Profile targets",
                    responses: { 200: { description: "Profiles" } }
                }
            },
            "/agent/gallery": {
                get: {
                    operationId: "listAgentGallery",
                    summary: "List R2 gallery images, defaulting to the AI gallery",
                    parameters: [{
                        name: "folder",
                        in: "query",
                        schema: { type: "string", default: "ai" }
                    }],
                    responses: { 200: { description: "Gallery files" } }
                }
            },
            "/agent/gallery/upload": {
                post: {
                    operationId: "uploadToAiGallery",
                    summary: "Upload an existing image into the AI gallery",
                    requestBody: {
                        required: true,
                        content: {
                            "application/json": {
                                schema: objectSchema({
                                    imageUrl: { type: "string" },
                                    imageBase64: { type: "string" },
                                    dataUrl: { type: "string" },
                                    mimeType: { type: "string" },
                                    filename: { type: "string" },
                                    folder: { type: "string", default: "ai" },
                                    addToProfile: { type: "boolean", default: true },
                                    serviceType: { type: "string" }
                                })
                            }
                        }
                    },
                    responses: { 200: { description: "Uploaded image" } }
                }
            },
            "/agent/images/generate": {
                post: {
                    operationId: "generateImagesToAiGallery",
                    summary: "Generate images, save them to the AI gallery, and optionally attach them to a profile",
                    requestBody: {
                        required: true,
                        content: {
                            "application/json": {
                                schema: objectSchema({
                                    prompt: { type: "string" },
                                    serviceType: { type: "string" },
                                    theme: { type: "string" },
                                    count: { type: "integer", minimum: 1, maximum: 10, default: 1 },
                                    size: { type: "string", default: "1536x1024" },
                                    quality: { type: "string", default: "high" },
                                    folder: { type: "string", default: "ai" },
                                    addToProfile: { type: "boolean", default: true }
                                })
                            }
                        }
                    },
                    responses: { 200: { description: "Generated images" } }
                }
            },
            "/agent/posts/now": {
                post: {
                    operationId: "postNowFromAgent",
                    summary: "Create a GBP post or photo upload now, optionally generating an image first",
                    requestBody: {
                        required: true,
                        content: {
                            "application/json": {
                                schema: objectSchema({
                                    postText: { type: "string" },
                                    mediaUrl: { type: "string" },
                                    generateImage: { type: "boolean" },
                                    prompt: { type: "string" },
                                    serviceType: { type: "string" },
                                    photoOnly: { type: "boolean", default: false },
                                    category: { type: "string", default: "ADDITIONAL" },
                                    cta: { type: "string" },
                                    linkUrl: { type: "string" }
                                })
                            }
                        }
                    },
                    responses: { 200: { description: "Posted result" } }
                }
            },
            "/agent/posts/daily": {
                post: {
                    operationId: "scheduleDailyAgentPosts",
                    summary: "Schedule daily GBP posts or photo-only uploads",
                    requestBody: {
                        required: true,
                        content: {
                            "application/json": {
                                schema: objectSchema({
                                    serviceType: { type: "string" },
                                    theme: { type: "string" },
                                    days: { type: "integer", minimum: 1, maximum: 30, default: 7 },
                                    time: { type: "string", default: "10:00" },
                                    startDate: { type: "string" },
                                    generateImages: { type: "boolean", default: true },
                                    generateImageEachDay: { type: "boolean", default: true },
                                    mediaUrls: { type: "array", items: { type: "string" } },
                                    photoOnly: { type: "boolean", default: false },
                                    category: { type: "string", default: "ADDITIONAL" },
                                    autoGenerateSummary: { type: "boolean", default: true }
                                })
                            }
                        }
                    },
                    responses: { 200: { description: "Scheduled items" } }
                }
            },
            "/agent/scheduler/enable": {
                post: {
                    operationId: "enableRecurringAgentScheduler",
                    summary: "Enable recurring posting for a profile and optionally prefill the AI photo pool",
                    requestBody: {
                        required: true,
                        content: {
                            "application/json": {
                                schema: objectSchema({
                                    serviceType: { type: "string" },
                                    theme: { type: "string" },
                                    time: { type: "string", default: "10:00" },
                                    cadence: {
                                        type: "string",
                                        enum: ["DAILY1", "DAILY2", "DAILY3", "WEEKLY1"],
                                        default: "DAILY1"
                                    },
                                    generateImages: { type: "boolean", default: false },
                                    imageCount: { type: "integer", minimum: 1, maximum: 30, default: 7 },
                                    size: { type: "string", default: "1536x1024" },
                                    quality: { type: "string", default: "high" }
                                })
                            }
                        }
                    },
                    responses: { 200: { description: "Scheduler enabled" } }
                }
            }
        }
    };
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

function getAgentAuthError(request, env) {
    const expected = String(env.AGENT_API_KEY || "").trim();
    if (!expected) {
        return { error: "AGENT_API_KEY not set", status: 500 };
    }
    const auth = request.headers.get("Authorization") || "";
    const bearer = auth.match(/^Bearer\s+(.+)$/i)?.[1] || "";
    const provided = bearer || request.headers.get("x-agent-api-key") || "";
    if (String(provided || "").trim() !== expected) {
        return { error: "Unauthorized", status: 401 };
    }
    return null;
}

function sanitizeFolder(raw) {
    return String(raw || "")
        .trim()
        .replace(/[^a-zA-Z0-9/_-]+/g, "-")
        .replace(/\/+/g, "/")
        .replace(/^\/+|\/+$/g, "")
        .replace(/(\.\.|\.)/g, "");
}

function sanitizeExtension(raw, fallback = ".jpg") {
    const ext = String(raw || "").toLowerCase().match(/\.[a-z0-9]+$/)?.[0] || fallback;
    return [".jpg", ".jpeg", ".png", ".webp"].includes(ext) ? ext : fallback;
}

function originMediaUrl(request, key) {
    const origin = new URL(request.url).origin.replace(/\/+$/, "");
    return origin + "/media/" + encodeURIComponent(key);
}

async function saveMediaBytes(request, env, arrayBuffer, options = {}) {
    const folder = sanitizeFolder(options.folder || "ai") || "ai";
    const filenameExt = sanitizeExtension(options.filename || "", ".jpg");
    const contentType = options.contentType || guessContentTypeFromExt(filenameExt);
    const key =
        folder +
        "/" +
        Date.now() +
        "-" +
        Math.random().toString(36).slice(2) +
        filenameExt;
    await env.MEDIA_BUCKET.put(key, arrayBuffer, {
        httpMetadata: { contentType }
    });
    return { key, url: originMediaUrl(request, key) };
}

async function generateAiImageToGallery(request, env, body = {}) {
    if (!env.OPENAI_API_KEY) {
        throw new Error("OPENAI_API_KEY not set");
    }
    const prompt = String(body.prompt || "").trim() || "home renovation photo";
    const requestedModel = String(
        body.model || env.OPENAI_IMAGE_MODEL || "gpt-image-1.5"
    ).trim();
    const requestedSize = String(body.size || "1536x1024").trim();
    const requestedQuality = String(body.quality || "high").trim();
    const folder = sanitizeFolder(body.folder || "ai") || "ai";

    async function saveImageBytes(arrayBuf, extra = {}) {
        const saved = await saveMediaBytes(request, env, arrayBuf, {
            folder,
            filename: ".jpg",
            contentType: "image/jpeg"
        });
        return {
            ...saved,
            prompt,
            size: extra.size || requestedSize,
            quality: extra.quality || requestedQuality
        };
    }

    async function callGptImage(model) {
        const openaiResp = await fetch("https://api.openai.com/v1/images/generations", {
            method: "POST",
            headers: {
                Authorization: `Bearer ${env.OPENAI_API_KEY}`,
                "Content-Type": "application/json"
            },
            body: JSON.stringify({
                model,
                prompt,
                size: requestedSize,
                quality: requestedQuality,
                output_format: "jpeg",
                output_compression: 88
            })
        });
        if (!openaiResp.ok) {
            const errText = await openaiResp.text().catch(() => "");
            throw new Error(errText || `OpenAI ${model} image request failed`);
        }
        const data = await openaiResp.json();
        const base64Image = data?.data?.[0]?.b64_json;
        if (!base64Image) {
            throw new Error(`No image bytes returned by ${model}`);
        }
        return {
            ...(await saveImageBytes(decodeBase64ToArrayBuffer(base64Image))),
            model
        };
    }

    async function callDalleFallback(fallbackFrom, cause) {
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
            throw new Error(
                `OpenAI error. ${fallbackFrom} failed: ${cause}; dall-e-3 failed: ${errText}`
            );
        }
        const data = await openaiResp.json();
        const imgUrl = data?.data?.[0]?.url;
        if (!imgUrl) throw new Error("No image returned by dall-e-3 fallback");

        const imgResp = await fetch(imgUrl);
        if (!imgResp.ok) {
            throw new Error("Failed to fetch generated dall-e-3 image");
        }
        return {
            ...(await saveImageBytes(await imgResp.arrayBuffer(), {
                size: "1024x1024",
                quality: "standard"
            })),
            model: "dall-e-3",
            fallbackFrom,
            fallbackReason: String(cause || "").slice(0, 500)
        };
    }

    const models = Array.from(
        new Set([requestedModel, "gpt-image-1", "dall-e-3"].filter(Boolean))
    );
    let lastError = null;
    for (const model of models) {
        try {
            if (model === "dall-e-3") {
                const fallbackFrom = requestedModel === "dall-e-3" ? "gpt-image-1" : requestedModel;
                return await callDalleFallback(fallbackFrom, lastError?.message || "GPT Image unavailable");
            }
            return await callGptImage(model);
        } catch (err) {
            lastError = err;
            console.warn("AI image model failed", model, err?.message || err);
        }
    }
    throw new Error(lastError?.message || "AI image failed");
}

function decodeAgentImageBody(body = {}) {
    const dataUrl = String(body.dataUrl || body.imageDataUrl || "").trim();
    let base64 = String(body.base64 || body.imageBase64 || "").trim();
    let mimeType = String(body.mimeType || body.contentType || "").trim();

    if (dataUrl) {
        const m = dataUrl.match(/^data:([^;]+);base64,(.+)$/i);
        if (!m) throw new Error("Invalid dataUrl");
        mimeType = mimeType || m[1];
        base64 = m[2];
    }

    if (!base64) throw new Error("Missing imageBase64 or dataUrl");
    const bytes = decodeBase64ToArrayBuffer(base64.replace(/\s+/g, ""));
    const ext =
        mimeType.includes("png") ? ".png" :
        mimeType.includes("webp") ? ".webp" :
        ".jpg";
    return {
        bytes,
        mimeType: mimeType || guessContentTypeFromExt(ext),
        ext
    };
}

function findAgentProfile(profiles, body = {}) {
    const profileId = String(body.profileId || "").trim();
    if (profileId) {
        const profile = profiles.find((p) => p && p.profileId === profileId);
        if (!profile) throw new Error("Profile not found");
        return profile;
    }

    const city = String(body.city || "").trim().toLowerCase();
    const query = String(body.profileQuery || body.businessName || body.query || "").trim().toLowerCase();
    const terms = [query, city].filter(Boolean);
    if (!terms.length) throw new Error("Missing profileId, profileQuery, or city");

    const scored = profiles
        .filter(Boolean)
        .map((p) => {
            const haystack = [
                p.profileId,
                p.businessName,
                p.city,
                p.storeCode,
                p.landingUrl,
                ...(Array.isArray(p.keywords) ? p.keywords : [])
            ].join(" ").toLowerCase();
            const score = terms.reduce((sum, term) => {
                if (!term) return sum;
                if (haystack.includes(term)) return sum + 3;
                const words = term.split(/\s+/).filter(Boolean);
                return sum + words.filter((w) => haystack.includes(w)).length;
            }, 0);
            return { profile: p, score };
        })
        .filter((item) => item.score > 0)
        .sort((a, b) => b.score - a.score);

    if (!scored.length) throw new Error("No matching profile found");
    return scored[0].profile;
}

function buildAgentImagePrompt(profile, body = {}, index = 0) {
    const service = String(body.serviceType || body.theme || body.topic || body.prompt || "home renovation service").trim();
    const city = String(body.city || profile.city || "").trim();
    const neighbourhoods = Array.isArray(profile.defaults?.photoNeighbourhoods) ?
        profile.defaults.photoNeighbourhoods :
        Array.isArray(profile.neighbourhoods) ? profile.neighbourhoods : [];
    const neighbourhood = String(body.neighbourhood || neighbourhoods[index % Math.max(1, neighbourhoods.length)] || "").trim();
    if (body.prompt) return String(body.prompt).trim();
    return [
        "Create an ultra-realistic human-shot contractor project photo for Google Business Profile.",
        `Business: ${profile.businessName || "local contractor"}.`,
        `Service theme: ${service}.`,
        `Location context: ${city}${neighbourhood ? `, ${neighbourhood}` : ""}.`,
        "Make the service instantly recognizable beside Google search results.",
        "Use a real job-site look with ordinary residential surroundings, natural light, tools, dust protection, and realistic imperfections.",
        "Do not include text, logos, watermarks, readable labels, fake before/after text, distorted rooms, or stock-photo styling."
    ].join(" ");
}

function buildDailyRunAt(index, body = {}) {
    const time = String(body.time || body.hhmm || "10:00").trim();
    const safeTime = /^\d{2}:\d{2}$/.test(time) ? time : "10:00";
    const start = body.startDate ? new Date(`${body.startDate}T${safeTime}:00`) : new Date();
    if (!body.startDate) {
        const [h, m] = safeTime.split(":").map((n) => parseInt(n, 10));
        start.setHours(h, m, 0, 0);
        if (start.getTime() < Date.now()) start.setDate(start.getDate() + 1);
    }
    start.setDate(start.getDate() + index);
    return start.toISOString();
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

    if (
        (pathname === "/privacy" || pathname === "/privacy-policy") &&
        request.method === "GET"
    ) {
        return textResponse(privacyPolicyHtml(), 200, {
            "Content-Type": "text/html; charset=utf-8"
        });
    }

    if (pathname === "/agent/openapi.json" && request.method === "GET") {
        return jsonResponse(getAgentOpenApiSchema(request));
    }

    if (pathname.startsWith("/agent/")) {
        const authError = getAgentAuthError(request, env);
        if (authError) return jsonResponse({ error: authError.error }, authError.status);

        try {
        if (pathname === "/agent/profiles" && request.method === "GET") {
            const profiles = await getProfiles(env);
            return jsonResponse({
                profiles: profiles.map((p) => ({
                    profileId: p.profileId,
                    businessName: p.businessName || "",
                    city: p.city || "",
                    storeCode: p.storeCode || "",
                    disabled: !!p.disabled,
                    photoPoolSize: Array.isArray(p.photoPool) ? p.photoPool.length : 0
                }))
            });
        }

        if (pathname === "/agent/gallery" && request.method === "GET") {
            const folder = sanitizeFolder(searchParams.get("folder") || "ai");
            const prefix = folder ? `${folder}/` : "";
            const list = await env.MEDIA_BUCKET.list({ prefix, limit: 500 });
            const files = (list?.objects || []).map((obj) => obj.key);
            return jsonResponse({
                folder,
                count: files.length,
                files,
                urls: files.map((key) => originMediaUrl(request, key))
            });
        }

        if (pathname === "/agent/gallery/upload" && request.method === "POST") {
            const body = await parseJsonBody(request);
            const profiles = await getProfiles(env);
            let profile = null;
            if (body.profileId || body.profileQuery || body.businessName || body.city || body.query) {
                profile = findAgentProfile(profiles, body);
            }

            let bytes;
            let mimeType;
            let ext;
            if (body.imageUrl) {
                const imageResp = await fetch(String(body.imageUrl));
                if (!imageResp.ok) {
                    return jsonResponse({ error: `Failed to fetch imageUrl (${imageResp.status})` }, 400);
                }
                bytes = await imageResp.arrayBuffer();
                mimeType = imageResp.headers.get("content-type") || "image/jpeg";
                ext =
                    mimeType.includes("png") ? ".png" :
                    mimeType.includes("webp") ? ".webp" :
                    ".jpg";
            } else {
                const decoded = decodeAgentImageBody(body);
                bytes = decoded.bytes;
                mimeType = decoded.mimeType;
                ext = decoded.ext;
            }

            const saved = await saveMediaBytes(request, env, bytes, {
                folder: body.folder || "ai",
                filename: body.filename || ext,
                contentType: mimeType
            });

            let updatedProfile = null;
            if (profile && body.addToProfile !== false) {
                updatedProfile = await appendPhotosToProfile(env, profile.profileId, [{
                    url: saved.url,
                    serviceType: body.serviceType || body.theme || "",
                    serviceTopicId: body.serviceTopicId || "",
                    captions: Array.isArray(body.captions) ? body.captions : []
                }]);
            }

            return jsonResponse({
                ok: true,
                ...saved,
                profileId: profile?.profileId || "",
                addedToProfile: !!updatedProfile
            });
        }

        if (pathname === "/agent/images/generate" && request.method === "POST") {
            const body = await parseJsonBody(request);
            const profiles = await getProfiles(env);
            const profile = findAgentProfile(profiles, body);
            const count = Math.max(1, Math.min(10, parseInt(body.count, 10) || 1));
            const generated = [];
            for (let i = 0; i < count; i++) {
                const prompt = buildAgentImagePrompt(profile, body, i);
                // eslint-disable-next-line no-await-in-loop
                const item = await generateAiImageToGallery(request, env, {
                    ...body,
                    prompt,
                    folder: body.folder || "ai"
                });
                generated.push(item);
            }
            if (body.addToProfile !== false) {
                await appendPhotosToProfile(env, profile.profileId, generated.map((item) => ({
                    url: item.url,
                    serviceType: body.serviceType || body.theme || body.topic || "",
                    serviceTopicId: body.serviceTopicId || ""
                })));
            }
            return jsonResponse({
                ok: true,
                profileId: profile.profileId,
                businessName: profile.businessName || "",
                generated
            });
        }

        if (pathname === "/agent/posts/now" && request.method === "POST") {
            const body = await parseJsonBody(request);
            const profiles = await getProfiles(env);
            const profile = findAgentProfile(profiles, body);
            let mediaUrl = ensureAbsoluteMediaUrl(env, body.mediaUrl || "");
            let generated = null;

            if (body.generateImage || (!mediaUrl && (body.prompt || body.serviceType || body.theme || body.topic))) {
                generated = await generateAiImageToGallery(request, env, {
                    ...body,
                    prompt: buildAgentImagePrompt(profile, body, 0),
                    folder: body.folder || "ai"
                });
                mediaUrl = generated.url;
                if (body.addToProfile !== false) {
                    await appendPhotosToProfile(env, profile.profileId, [{
                        url: mediaUrl,
                        serviceType: body.serviceType || body.theme || body.topic || "",
                        serviceTopicId: body.serviceTopicId || ""
                    }]);
                }
            }

            if (body.photoOnly) {
                const result = await uploadPhotoToGmb(env, profile, {
                    mediaUrl,
                    caption: body.caption || body.postText || "",
                    category: body.category || "ADDITIONAL"
                });
                return jsonResponse({ ok: true, mode: "photo", profileId: profile.profileId, mediaUrl, generated, result });
            }

            const { postToGmb } = await
            import ("./gmb.mjs");
            const result = await postToGmb(env, {
                ...body,
                profileId: profile.profileId,
                mediaUrl,
                serviceType: body.serviceType || body.theme || body.topic || ""
            });
            return jsonResponse({ ok: true, mode: "post", profileId: profile.profileId, mediaUrl, generated, result });
        }

        if (pathname === "/agent/posts/daily" && request.method === "POST") {
            const body = await parseJsonBody(request);
            const profiles = await getProfiles(env);
            const profile = findAgentProfile(profiles, body);
            const count = Math.max(1, Math.min(30, parseInt(body.days || body.count, 10) || 7));
            const mediaUrls = Array.isArray(body.mediaUrls) ? body.mediaUrls.slice() : [];
            const generated = [];
            const scheduled = [];

            for (let i = 0; i < count; i++) {
                let mediaUrl = ensureAbsoluteMediaUrl(env, mediaUrls[i] || mediaUrls[i % Math.max(1, mediaUrls.length)] || "");
                if (body.generateImages !== false && (!mediaUrl || body.generateImageEachDay !== false)) {
                    const prompt = buildAgentImagePrompt(profile, body, i);
                    // eslint-disable-next-line no-await-in-loop
                    const item = await generateAiImageToGallery(request, env, {
                        ...body,
                        prompt,
                        folder: body.folder || "ai"
                    });
                    generated.push(item);
                    mediaUrl = item.url;
                }

                if (!mediaUrl) {
                    return jsonResponse({ error: "No mediaUrl available; provide mediaUrls or allow generateImages" }, 400);
                }

                const runAt = buildDailyRunAt(i, body);
                if (body.photoOnly) {
                    // eslint-disable-next-line no-await-in-loop
                    const item = await enqueueScheduledPhoto(env, {
                        profileId: profile.profileId,
                        runAt,
                        body: {
                            mediaUrl,
                            caption: body.caption || "",
                            category: body.category || "ADDITIONAL"
                        }
                    });
                    scheduled.push(item);
                } else {
                    // eslint-disable-next-line no-await-in-loop
                    const item = await enqueueScheduledPost(env, {
                        profileId: profile.profileId,
                        runAt,
                        body: {
                            ...body,
                            profileId: profile.profileId,
                            mediaUrl,
                            serviceType: body.serviceType || body.theme || body.topic || "",
                            autoGenerateSummary: body.autoGenerateSummary !== false
                        }
                    });
                    scheduled.push(item);
                }
            }

            if (generated.length && body.addToProfile !== false) {
                await appendPhotosToProfile(env, profile.profileId, generated.map((item) => ({
                    url: item.url,
                    serviceType: body.serviceType || body.theme || body.topic || "",
                    serviceTopicId: body.serviceTopicId || ""
                })));
            }

            return jsonResponse({
                ok: true,
                profileId: profile.profileId,
                businessName: profile.businessName || "",
                mode: body.photoOnly ? "photo" : "post",
                generated,
                scheduled
            });
        }

        if (pathname === "/agent/scheduler/enable" && request.method === "POST") {
            const body = await parseJsonBody(request);
            const profiles = await getProfiles(env);
            const profile = findAgentProfile(profiles, body);
            const time = /^\d{2}:\d{2}$/.test(String(body.time || "")) ?
                String(body.time) :
                "10:00";
            const cadence = String(body.cadence || "DAILY1").toUpperCase();
            const safeCadence = ["DAILY1", "DAILY2", "DAILY3", "WEEKLY1"].includes(cadence) ?
                cadence :
                "DAILY1";
            const intervalDays =
                safeCadence === "DAILY2" ? 2 :
                safeCadence === "DAILY3" ? 3 :
                safeCadence === "WEEKLY1" ? 7 :
                1;
            const generated = [];

            if (body.generateImages) {
                const imageCount = Math.max(1, Math.min(30, parseInt(body.imageCount || body.days || body.count, 10) || 7));
                for (let i = 0; i < imageCount; i++) {
                    const prompt = buildAgentImagePrompt(profile, body, i);
                    // eslint-disable-next-line no-await-in-loop
                    const item = await generateAiImageToGallery(request, env, {
                        ...body,
                        prompt,
                        folder: body.folder || "ai"
                    });
                    generated.push(item);
                }
                await appendPhotosToProfile(env, profile.profileId, generated.map((item) => ({
                    url: item.url,
                    serviceType: body.serviceType || body.theme || body.topic || "",
                    serviceTopicId: body.serviceTopicId || ""
                })));
            }

            const current = await getSchedulerConfig(env);
            const config = await setSchedulerConfig(env, {
                enabled: true,
                perProfileTimes: {
                    ...(current.perProfileTimes || {}),
                    [profile.profileId]: time
                },
                perProfileIntervalDays: {
                    ...(current.perProfileIntervalDays || {}),
                    [profile.profileId]: intervalDays
                },
                perProfileCadence: {
                    ...(current.perProfileCadence || {}),
                    [profile.profileId]: safeCadence
                }
            });

            return jsonResponse({
                ok: true,
                profileId: profile.profileId,
                businessName: profile.businessName || "",
                scheduledTime: time,
                cadence: safeCadence,
                generated,
                config
            });
        }

        return jsonResponse({ error: "Agent endpoint not found" }, 404);
        } catch (e) {
            return jsonResponse({ error: e.message || "Agent request failed" }, 400);
        }
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
        if (body.hasOwnProperty("mediaUrl")) {
            defaults.mediaUrl = ensureAbsoluteMediaUrl(env, body.mediaUrl || "");
        }
        if (body.hasOwnProperty("overlayUrl")) {
            defaults.overlayUrl = ensureAbsoluteMediaUrl(env, body.overlayUrl || "");
        }
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
        if (body.hasOwnProperty("serviceTopics")) {
            target.serviceTopics = Array.isArray(body.serviceTopics) ? body.serviceTopics : [];
        }
        if (body.hasOwnProperty("defaultServiceTopicId")) {
            target.defaultServiceTopicId = String(body.defaultServiceTopicId || "").trim();
        }
        if (body.hasOwnProperty("mediaTopics")) {
            target.mediaTopics =
                body.mediaTopics && typeof body.mediaTopics === "object" ?
                body.mediaTopics :
                {};
        }

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
        const serviceTopicId = searchParams.get("serviceTopicId") || "";
        const serviceTopics = Array.isArray(profile.serviceTopics) ? profile.serviceTopics : [];
        const topic =
            serviceTopicId ?
            serviceTopics.find((t) => t && t.id === serviceTopicId) :
            null;
        const defaults = profile.defaults || {};
        const cityOverride = String(searchParams.get("city") || "").trim();
        const neighbourhoodOverride = String(searchParams.get("neighbourhood") || "").trim();
        const photoKeywordsOverride = String(searchParams.get("photoKeywords") || "").trim();
        const photoCategoriesOverride = String(searchParams.get("photoCategories") || "").trim();
        const profileForGeneration = {
            ...profile,
            city: cityOverride || profile.city || "",
            defaults: {
                ...defaults,
                photoKeywords: photoKeywordsOverride || defaults.photoKeywords || "",
                photoCategories: photoCategoriesOverride || defaults.photoCategories || ""
            }
        };
        const overrides = topic ?
            {
                serviceType: topic.serviceType || topic.label || "",
                serviceSummary: topic.summary || "",
                serviceNotes: topic.notes || "",
                neighbourhood: neighbourhoodOverride
            } :
            { neighbourhood: neighbourhoodOverride };

        const basics = await fetchLocationBasics(env, profile);
        const built = await composeAiTemplatePost(env, profileForGeneration, overrides, basics);
        const quickLines = buildQuickLinkLines(defaults);
        let post = insertQuickLinksBeforeHashtags((built.summary || "").trim(), quickLines);
        const topicHashtags = topic && Array.isArray(topic.hashtags) ? topic.hashtags : [];
        const hashtags = Array.from(
            new Set([...(built.hashtags || []), ...topicHashtags])
        );
        if (hashtags.length) {
            const spaceLeft = 1450 - post.length;
            if (spaceLeft > 20) {
                const tagLine = safeJoinHashtags(hashtags, spaceLeft);
                if (tagLine && post.length + 2 + tagLine.length <= 1450) {
                    post += "\n\n" + tagLine;
                }
            }
        }
        return jsonResponse({
            profileId,
            businessName: profile.businessName || "",
            city: profile.city || "",
            neighbourhood: built.neighbourhood || "",
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

    if (pathname === "/performance" && request.method === "GET") {
        const urlObj = new URL(request.url);
        const profileId = urlObj.searchParams.get("profileId") || "";
        const days = Number(urlObj.searchParams.get("days") || "30");
        const startMonth = urlObj.searchParams.get("startMonth") || "";
        const endMonth = urlObj.searchParams.get("endMonth") || "";
        try {
            const result = await fetchPerformanceMetrics(env, profileId, { days, startMonth, endMonth });
            return jsonResponse(result);
        } catch (e) {
            return jsonResponse({ error: e.message || "Failed to fetch performance" }, 400);
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
            console.log("[photo-now] start", {
                profileId,
                locationId: profile.locationId || "",
                mediaUrl
            });
            const result = await uploadPhotoToGmb(env, profile, {
                mediaUrl,
                caption: body.caption || "",
                category: body.category || "ADDITIONAL"
            });
            console.log("[photo-now] success", {
                profileId,
                locationId: profile.locationId || "",
                mediaItemName: result && result.name ? result.name : ""
            });
            return jsonResponse({ ok: true, result });
        } catch (e) {
            console.error("[photo-now] failed", {
                profileId,
                locationId: profile.locationId || "",
                mediaUrl,
                error: e && e.message ? e.message : String(e)
            });
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

    if (pathname === "/photo-scheduled/run-due" && request.method === "POST") {
        try {
            const result = await runDueScheduledPhotos(env);
            return jsonResponse(result);
        } catch (e) {
            return jsonResponse({ error: e.message || "Failed to run due photo queue" }, 400);
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
        const requestedModel = String(
            body.model || env.OPENAI_IMAGE_MODEL || "gpt-image-1.5"
        ).trim();
        const requestedSize = String(body.size || "1536x1024").trim();
        const requestedQuality = String(body.quality || "high").trim();
        const origin = new URL(request.url).origin.replace(/\/+$/, "");
        const key =
            "ai/" +
            Date.now() +
            "-" +
            Math.random().toString(36).slice(2) +
            ".jpg";

        async function saveImageBytes(arrayBuf) {
            await env.MEDIA_BUCKET.put(key, arrayBuf, {
                httpMetadata: { contentType: "image/jpeg" }
            });
            const url = origin + "/media/" + encodeURIComponent(key);
            return { url, key };
        }

        async function callGptImage(model) {
            const openaiResp = await fetch("https://api.openai.com/v1/images/generations", {
                method: "POST",
                headers: {
                    Authorization: `Bearer ${env.OPENAI_API_KEY}`,
                    "Content-Type": "application/json"
                },
                body: JSON.stringify({
                    model,
                    prompt,
                    size: requestedSize,
                    quality: requestedQuality,
                    output_format: "jpeg",
                    output_compression: 88
                })
            });
            if (!openaiResp.ok) {
                const errText = await openaiResp.text().catch(() => "");
                throw new Error(errText || `OpenAI ${model} image request failed`);
            }
            const data = await openaiResp.json();
            const base64Image = data?.data?.[0]?.b64_json;
            if (!base64Image) {
                throw new Error(`No image bytes returned by ${model}`);
            }
            const saved = await saveImageBytes(decodeBase64ToArrayBuffer(base64Image));
            return {
                ...saved,
                prompt,
                model,
                size: requestedSize,
                quality: requestedQuality
            };
        }

        async function callDalleFallback(fallbackFrom, cause) {
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
                throw new Error(
                    `OpenAI error. ${fallbackFrom} failed: ${cause}; dall-e-3 failed: ${errText}`
                );
            }
            const data = await openaiResp.json();
            const imgUrl = data?.data?.[0]?.url;
            if (!imgUrl) throw new Error("No image returned by dall-e-3 fallback");

            const imgResp = await fetch(imgUrl);
            if (!imgResp.ok) {
                throw new Error("Failed to fetch generated dall-e-3 image");
            }
            const saved = await saveImageBytes(await imgResp.arrayBuffer());
            return {
                ...saved,
                prompt,
                model: "dall-e-3",
                size: "1024x1024",
                quality: "standard",
                fallbackFrom,
                fallbackReason: String(cause || "").slice(0, 500)
            };
        }

        try {
            const models = Array.from(
                new Set([requestedModel, "gpt-image-1", "dall-e-3"].filter(Boolean))
            );
            let lastError = null;
            for (const model of models) {
                try {
                    if (model === "dall-e-3") {
                        const fallbackFrom = requestedModel === "dall-e-3" ? "gpt-image-1" : requestedModel;
                        return jsonResponse(
                            await callDalleFallback(fallbackFrom, lastError?.message || "GPT Image unavailable")
                        );
                    }
                    return jsonResponse(await callGptImage(model));
                } catch (err) {
                    lastError = err;
                    console.warn("AI image model failed", model, err?.message || err);
                }
            }
            return jsonResponse({ error: lastError?.message || "AI image failed" }, 500);
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
