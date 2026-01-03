import { getJson, setJson } from "./storage.mjs";
import { callBusinessProfileAPI, callBusinessProfileApi, getAccessToken } from "./google.mjs";
import { aiGenerateSummaryAndHashtags, pickNeighbourhood, safeJoinHashtags } from "./ai.mjs";

const DEFAULT_SCHED = {
    enabled: false,
    defaultTime: "10:00",
    tickSeconds: 30,
    defaultIntervalDays: 1,
    defaultCadence: "DAILY1", // DAILY1, DAILY2, DAILY3, WEEKLY1
    perProfileTimes: {},
    perProfileIntervalDays: {},
    perProfileCadence: {}
};
const AUTO_QUEUE_KEY = "autoSchedulerQueue";
const DEFAULT_AUTO_PER_TICK = 2;

const TEMPLATE_CYCLE = ["SERVICE", "OFFER", "TIP", "SOCIAL_PROOF"];
const CTA_LABELS = {
    CALL_NOW: "Call now",
    LEARN_MORE: "Learn more",
    BOOK: "Book",
    ORDER: "Order",
    SHOP: "Shop",
    SIGN_UP: "Sign up"
};
const DEFAULT_MEDIA_BASE = "https://gmb-automation-backend.webtoronto22.workers.dev";

async function ensureKvTable(env) {
    await env.D1_DB.prepare(`
    CREATE TABLE IF NOT EXISTS kv (
      key TEXT PRIMARY KEY,
      value TEXT,
      updated_at TEXT NOT NULL
    );
  `).run();
}

function randomId(prefix = "svc") {
    if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
        return `${prefix}-${crypto.randomUUID()}`;
    }
    return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

function normalizeServiceTopicsList(list = [], desiredDefaultId = "") {
    if (!Array.isArray(list)) return { items: [], defaultId: "" };
    const items = [];
    const seenIds = new Set();
    let explicitDefaultId = "";
    list.forEach((topic, idx) => {
        if (!topic || typeof topic !== "object") return;
        const label = String(topic.label || topic.name || topic.serviceType || "").trim();
        if (!label) return;
        let id = String(topic.id || topic.key || "").trim();
        if (!id) {
            const slug = label.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
            id = slug ? `${slug}-${idx}` : randomId("svc");
        }
        if (seenIds.has(id)) {
            id = `${id}-${idx}`;
        }
        seenIds.add(id);
        const summary = String(topic.summary || topic.body || "").trim();
        const hashtags = Array.isArray(topic.hashtags) ?
            topic.hashtags
            .map((tag) => String(tag || "").trim())
            .filter(Boolean) :
            [];
        const entry = {
            id,
            label,
            serviceType: String(topic.serviceType || label || "").trim(),
            summary,
            hashtags,
            notes: String(topic.notes || "").trim(),
            isDefault: !!topic.isDefault
        };
        if (entry.isDefault) {
            explicitDefaultId = entry.id;
        }
        items.push(entry);
    });
    let defaultId = desiredDefaultId || explicitDefaultId || "";
    if (defaultId && !items.some((topic) => topic.id === defaultId)) {
        defaultId = "";
    }
    if (!defaultId && items.length) {
        const flagged = items.find((topic) => topic.isDefault);
        defaultId = flagged ? flagged.id : items[0].id;
    }
    const normalized = items.map((topic) => ({
        ...topic,
        isDefault: topic.id === defaultId
    }));
    return { items: normalized, defaultId: defaultId || "" };
}

function getProfileServiceTopics(profile) {
    return Array.isArray(profile && profile.serviceTopics) ? profile.serviceTopics : [];
}

function findServiceTopic(profile, topicId) {
    if (!topicId) return null;
    const list = getProfileServiceTopics(profile);
    return list.find((topic) => topic && topic.id === topicId) || null;
}

function getDefaultServiceTopic(profile) {
    const list = getProfileServiceTopics(profile);
    if (!list.length) return null;
    const targetId = profile && profile.defaultServiceTopicId;
    if (targetId) {
        const match = list.find((topic) => topic && topic.id === targetId);
        if (match) return match;
    }
    const flagged = list.find((topic) => topic && topic.isDefault);
    return flagged || list[0];
}

function normalizeMediaTopicsMap(env, map = {}) {
    if (!map || typeof map !== "object") return {};
    const out = {};
    Object.entries(map).forEach(([key, val]) => {
        if (!val) return;
        const normalizedUrl = ensureAbsoluteMediaUrl(env, key);
        const topicId = String(val || "").trim();
        if (!normalizedUrl || !topicId) return;
        out[normalizedUrl] = topicId;
    });
    return out;
}

function normalizeProfiles(list) {
    if (!Array.isArray(list)) return [];
    return list.map((p) => {
        const out = {...(p || {}) };
        if (typeof out.disabled !== "boolean") out.disabled = false;
        if (!Array.isArray(out.neighbourhoods)) out.neighbourhoods = [];
        if (!Array.isArray(out.keywords)) out.keywords = [];
        if (!Array.isArray(out.photoPool)) out.photoPool = [];
        if (!Array.isArray(out.serviceTopics)) out.serviceTopics = [];
        if (!out.mediaTopics || typeof out.mediaTopics !== "object") out.mediaTopics = {};
        const normalizedTopics = normalizeServiceTopicsList(out.serviceTopics, out.defaultServiceTopicId || "");
        out.serviceTopics = normalizedTopics.items;
        out.defaultServiceTopicId = normalizedTopics.defaultId;
        if (!out.profileId) out.profileId = out.locationId || "";
        return out;
    });
}

function normalizeProfileMedia(env, profile) {
    if (!profile || typeof profile !== "object") return profile;
    const out = {...profile };
    if (out.defaults && typeof out.defaults === "object") {
        const defaults = {...out.defaults };
        if (defaults.mediaUrl) {
            const before = defaults.mediaUrl;
            defaults.mediaUrl = ensureAbsoluteMediaUrl(env, defaults.mediaUrl);
            if (!defaults.mediaUrl.startsWith("http")) {
                console.warn("normalizeProfileMedia: defaults.mediaUrl stayed relative", { profileId: out.profileId, before, after: defaults.mediaUrl });
            }
        }
        if (defaults.overlayUrl) {
            const before = defaults.overlayUrl;
            defaults.overlayUrl = ensureAbsoluteMediaUrl(env, defaults.overlayUrl);
            if (!defaults.overlayUrl.startsWith("http")) {
                console.warn("normalizeProfileMedia: defaults.overlayUrl stayed relative", { profileId: out.profileId, before, after: defaults.overlayUrl });
            }
        }
        out.defaults = defaults;
    }
    if (Array.isArray(out.photoPool)) {
        out.photoPool = out.photoPool.map((entry) => {
            if (!entry) return entry;
            if (typeof entry === "string") {
                const before = entry;
                const full = ensureAbsoluteMediaUrl(env, entry);
                if (!/^https?:\/\//i.test(full)) {
                    console.warn("normalizeProfileMedia: photoPool entry stayed relative", { profileId: out.profileId, before, after: full });
                }
                return full || entry;
            }
            if (entry && typeof entry === "object") {
                const before = entry.url || "";
                const full = ensureAbsoluteMediaUrl(env, entry.url || "");
                if (!/^https?:\/\//i.test(full)) {
                    console.warn("normalizeProfileMedia: photoPool object.url stayed relative", { profileId: out.profileId, before, after: full });
                }
                return {...entry, url: full || entry.url };
            }
            return entry;
        });
    }
    if (out.mediaTopics && typeof out.mediaTopics === "object") {
        out.mediaTopics = normalizeMediaTopicsMap(env, out.mediaTopics);
    }
    return out;
}

export function isProfileActive(profile) {
    return !!(profile && profile.profileId && profile.disabled !== true);
}

export async function getProfiles(env) {
    const raw = (await getJson(env, "profiles", [])) || [];
    return normalizeProfiles(raw).map((p) => normalizeProfileMedia(env, p));
}

export async function saveProfiles(env, list) {
    const normalized = normalizeProfiles(list).map((p) => normalizeProfileMedia(env, p));
    await setJson(env, "profiles", normalized);
}

// --- D1 scheduled posts helpers ---
async function ensureScheduledTable(env) {
    await env.D1_DB.prepare(`
    CREATE TABLE IF NOT EXISTS scheduled_posts (
      id TEXT PRIMARY KEY,
      profile_id TEXT NOT NULL,
      run_at TEXT NOT NULL,
      created_at TEXT NOT NULL,
      body_json TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'QUEUED',
      posted_at TEXT,
      last_url TEXT
    );
  `).run();
}

async function readScheduledRows(env, includeAll = false) {
    await ensureScheduledTable(env);
    const where = includeAll ? "" : "WHERE status = 'QUEUED'";
    const { results } = await env.D1_DB.prepare(
        `SELECT id, profile_id, run_at, created_at, body_json, status, posted_at, last_url
     FROM scheduled_posts
     ${where}
     ORDER BY datetime(run_at) ASC`
    ).all();
    return (results || []).map((row) => ({
        id: row.id,
        profileId: row.profile_id,
        runAt: row.run_at,
        createdAt: row.created_at,
        body: row.body_json ? normalizeBodyMedia(env, JSON.parse(row.body_json)) : {},
        status: row.status || "QUEUED",
        postedAt: row.posted_at || null,
        lastUrl: row.last_url || ""
    }));
}

async function upsertScheduledRow(env, item) {
    await ensureScheduledTable(env);
    const normalizedBody = normalizeBodyMedia(env, item.body || {});
    const bodyJson = JSON.stringify(normalizedBody);
    await env.D1_DB.prepare(
            `
    INSERT INTO scheduled_posts (id, profile_id, run_at, created_at, body_json, status, posted_at, last_url)
    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
    ON CONFLICT(id) DO UPDATE SET
      profile_id = excluded.profile_id,
      run_at = excluded.run_at,
      created_at = excluded.created_at,
      body_json = excluded.body_json,
      status = excluded.status,
      posted_at = excluded.posted_at,
      last_url = excluded.last_url
  `
        )
        .bind(
            item.id,
            item.profileId,
            item.runAt,
            item.createdAt || new Date().toISOString(),
            bodyJson,
            item.status || "QUEUED",
            item.postedAt || null,
            item.lastUrl || ""
        )
        .run();
}

async function deleteScheduledRow(env, id) {
    await ensureScheduledTable(env);
    await env.D1_DB.prepare("DELETE FROM scheduled_posts WHERE id = ?1").bind(id).run();
}

async function markScheduledPosted(env, id, lastUrl = "") {
    await ensureScheduledTable(env);
    await env.D1_DB.prepare(
            `UPDATE scheduled_posts SET status = 'POSTED', posted_at = ?2, last_url = ?3 WHERE id = ?1`
        )
        .bind(id, new Date().toISOString(), lastUrl || "")
        .run();
}

export async function getScheduledPosts(env) {
    return readScheduledRows(env, false);
}

export async function getAllScheduledPosts(env) {
    return readScheduledRows(env, true);
}

export async function saveScheduledPosts(env, list) {
    await ensureScheduledTable(env);
    for (const item of list) {
        const id = item.id || crypto.randomUUID();
        const body = normalizeBodyMedia(env, item.body || {});
        await env.D1_DB.prepare(
                `
      INSERT INTO scheduled_posts (id, profile_id, run_at, created_at, body_json, status)
      VALUES (?1, ?2, ?3, ?4, ?5, 'QUEUED')
      ON CONFLICT(id) DO UPDATE SET
        profile_id = excluded.profile_id,
        run_at = excluded.run_at,
        created_at = excluded.created_at,
        body_json = excluded.body_json,
        status = 'QUEUED'
    `
            )
            .bind(
                id,
                item.profileId,
                item.runAt,
                item.createdAt || new Date().toISOString(),
                JSON.stringify(body)
            )
            .run();
    }
}

// --- Photo scheduled helpers ---
async function ensurePhotoTable(env) {
    await env.D1_DB.prepare(`
    CREATE TABLE IF NOT EXISTS scheduled_photos (
      id TEXT PRIMARY KEY,
      profile_id TEXT NOT NULL,
      run_at TEXT NOT NULL,
      created_at TEXT NOT NULL,
      body_json TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'QUEUED',
      posted_at TEXT,
      last_error TEXT
    );
  `).run();

    // Add last_error column for existing deployments
    try {
        await env.D1_DB.prepare(
            `ALTER TABLE scheduled_photos ADD COLUMN last_error TEXT`
        ).run();
    } catch (e) {
        // ignore if it already exists
        if (!/duplicate column|already exists/i.test(String(e && e.message))) {
            console.warn("ensurePhotoTable alter failed", e);
        }
    }
}

async function readPhotoRows(env, includeAll = false) {
    await ensurePhotoTable(env);
    const where = includeAll ? "" : "WHERE status = 'QUEUED'";
    const { results } = await env.D1_DB.prepare(
        `SELECT id, profile_id, run_at, created_at, body_json, status, posted_at, last_error
     FROM scheduled_photos
     ${where}
     ORDER BY datetime(run_at) ASC`
    ).all();
    return (results || []).map((row) => ({
        id: row.id,
        profileId: row.profile_id,
        runAt: row.run_at,
        createdAt: row.created_at,
        body: row.body_json ? normalizeBodyMedia(env, JSON.parse(row.body_json)) : {},
        status: row.status || "QUEUED",
        postedAt: row.posted_at || null,
        lastError: row.last_error || ""
    }));
}

async function upsertPhotoRow(env, item) {
    await ensurePhotoTable(env);
    const normalizedBody = normalizeBodyMedia(env, item.body || {});
    const bodyJson = JSON.stringify(normalizedBody);
    await env.D1_DB.prepare(
            `
    INSERT INTO scheduled_photos (id, profile_id, run_at, created_at, body_json, status, posted_at, last_error)
    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
    ON CONFLICT(id) DO UPDATE SET
      profile_id = excluded.profile_id,
      run_at = excluded.run_at,
      created_at = excluded.created_at,
      body_json = excluded.body_json,
      status = excluded.status,
      posted_at = excluded.posted_at,
      last_error = excluded.last_error
  `
        )
        .bind(
            item.id,
            item.profileId,
            item.runAt,
            item.createdAt || new Date().toISOString(),
            bodyJson,
            item.status || "QUEUED",
            item.postedAt || null,
            item.lastError || ""
        )
        .run();
}

async function deletePhotoRow(env, id) {
    await ensurePhotoTable(env);
    await env.D1_DB.prepare("DELETE FROM scheduled_photos WHERE id = ?1").bind(id).run();
}

async function markPhotoPosted(env, id) {
    await ensurePhotoTable(env);
    await env.D1_DB.prepare(
            `UPDATE scheduled_photos SET status = 'POSTED', posted_at = ?2 WHERE id = ?1`
        )
        .bind(id, new Date().toISOString())
        .run();
}

async function markPhotoFailed(env, id, errorText = "") {
    await ensurePhotoTable(env);
    await env.D1_DB.prepare(
            `UPDATE scheduled_photos SET status = 'FAILED', last_error = ?2 WHERE id = ?1`
        )
        .bind(id, String(errorText || "").slice(0, 500))
        .run();
}

export async function getScheduledPhotos(env) {
    return readPhotoRows(env, false);
}

export async function getAllScheduledPhotos(env) {
    return readPhotoRows(env, true);
}

export async function enqueueScheduledPhoto(env, payload) {
    const id = crypto.randomUUID();
    const runAt = new Date(payload.runAt).toISOString();
    const item = {
        id,
        runAt,
        createdAt: new Date().toISOString(),
        profileId: payload.profileId,
        body: normalizeBodyMedia(env, payload.body || {}),
        status: "QUEUED"
    };
    await upsertPhotoRow(env, item);
    return item;
}

export async function saveScheduledPhotos(env, list) {
    await ensurePhotoTable(env);
    for (const item of list) {
        if (!item || !item.profileId || !item.runAt) continue;
        const id = item.id || crypto.randomUUID();
        await upsertPhotoRow(env, {
            id,
            profileId: item.profileId,
            runAt: item.runAt,
            createdAt: item.createdAt || new Date().toISOString(),
            body: normalizeBodyMedia(env, item.body || {}),
            status: item.status || "QUEUED"
        });
    }
    return list.filter(Boolean);
}

export async function deletePhotoScheduled(env, id) {
    await deletePhotoRow(env, id);
}

export async function commitScheduledPosts(env, items) {
    const normalized = Array.isArray(items) ?
        items
        .map((it) => ({
            id: it.id || crypto.randomUUID(),
            runAt: new Date(it.runAt).toISOString(),
            createdAt: it.createdAt || new Date().toISOString(),
            profileId: it.profileId,
            body: normalizeBodyMedia(env, it.body || {}),
            status: it.status || "QUEUED"
        }))
        .filter((it) => it.profileId && it.runAt) : [];
    const existing = await getScheduledPosts(env);
    await saveScheduledPosts(env, existing.concat(normalized));
    return normalized.length;
}

// --- Template cycle helpers ---
async function ensureCycleState(env) {
    await ensureKvTable(env);
}

async function getCycleState(env) {
    await ensureCycleState(env);
    const row = await env.D1_DB.prepare("SELECT value FROM kv WHERE key = ?").bind("cycleState").first();
    if (!row || !row.value) return {};
    try {
        const parsed = JSON.parse(row.value);
        return parsed && typeof parsed === "object" ? parsed : {};
    } catch {
        return {};
    }
}

async function saveCycleState(env, state) {
    await ensureCycleState(env);
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
        .bind("cycleState", JSON.stringify(state || {}), now)
        .run();
}

function uniqueArray(arr) {
    return Array.from(new Set(arr.filter(Boolean)));
}

function buildHashtags(keywords = [], city = "", service = "") {
    const tags = [];
    const safeCity = city ? city.replace(/\s+/g, "") : "";
    const baseKeywords = keywords.slice(0, 6).map((k) => k.replace(/\s+/g, ""));
    baseKeywords.forEach((kw) => {
        tags.push("#" + kw);
        if (safeCity) tags.push("#" + safeCity + kw);
    });
    if (safeCity) {
        tags.push("#" + safeCity);
        tags.push("#" + safeCity + "Local");
    }
    if (service) {
        const svc = service.replace(/\s+/g, "");
        tags.push("#" + svc);
        if (safeCity) tags.push("#" + safeCity + svc);
    }
    const unique = uniqueArray(tags);
    const max = Math.min(12, Math.max(8, unique.length));
    return unique.slice(0, max);
}

async function buildTemplatePost(env, profile, overrides = {}, basics = {}) {
    const state = await getCycleState(env);
    const entry = state[profile.profileId] || { idx: 0, lastUrl: "" };
    const idx = entry.idx || 0;
    const template = TEMPLATE_CYCLE[idx % TEMPLATE_CYCLE.length];

    const baseKeywords = Array.isArray(profile.keywords) ? profile.keywords.filter(Boolean) : [];
    const overrideService = typeof overrides.serviceType === "string" ? overrides.serviceType.trim() : "";
    const keywords = overrideService ?
        uniqueArray([overrideService, ...baseKeywords]) :
        baseKeywords;
    const city = profile.city || profile.region || "";
    const primaryKw = keywords[0] ||
        overrideService ||
        profile.businessName ||
        "local services";
    const prevUrl = entry.lastUrl || "";
    const defaults = profile.defaults || {};

    const site =
        overrides.linkUrl ||
        (profile.defaults && profile.defaults.linkUrl) ||
        basics.websiteUri ||
        profile.landingUrl ||
        "";
    const ctaCode = overrides.cta || (profile.defaults && profile.defaults.cta) || "LEARN_MORE";
    const ctaLabel = CTA_LABELS[ctaCode] || "Learn more";

    const lines = [];
    const keywordLine = keywords.length ?
        `${city ? city + " • " : ""}${keywords.slice(0, 3).join(" · ")}` :
        `${city || "Local"} • ${primaryKw}`;
    lines.push(keywordLine);

    const business = profile.businessName || primaryKw;
    const serviceLower = primaryKw.toLowerCase();
    const templateCtx = {
        business,
        city: city || "your area",
        service: primaryKw,
        serviceLower,
        serviceVerb: serviceLower
    };

    const introTemplate = pickTemplateValue(INTRO_TEMPLATES, profile.profileId, `intro:${idx}`);
    if (introTemplate) {
        lines.push(formatTemplate(introTemplate, templateCtx));
    }

    const templateList = TEMPLATE_MESSAGES[template] || TEMPLATE_MESSAGES.default;
    const messageTemplate = pickTemplateValue(templateList, profile.profileId, `message:${idx}`);
    if (messageTemplate) {
        lines.push(formatTemplate(messageTemplate, templateCtx));
    }

    const detailOne = pickTemplateValue(DETAIL_OPTIONS_ONE, profile.profileId, `detail1:${idx}`);
    const detailTwo = pickTemplateValue(DETAIL_OPTIONS_TWO, profile.profileId, `detail2:${idx}`);
    const differentiatorTemplate = pickTemplateValue(
        DIFFERENTIATOR_TEMPLATES,
        profile.profileId,
        `diff:${idx}`
    );
    if (differentiatorTemplate) {
        lines.push(
            formatTemplate(differentiatorTemplate, {
                ...templateCtx,
                detailOne,
                detailTwo
            })
        );
    }

    if (template === "OFFER" && overrides.offerTitle) {
        lines.push(`Special: ${overrides.offerTitle}`);
    }
    if (prevUrl) {
        lines.push(`Previous update: ${prevUrl}`);
    }
    if (site) {
        lines.push(`More info: ${site}`);
    }
    const hashtags = buildHashtags(keywords, city, primaryKw);
    const summary = lines.filter(Boolean).join("\n").slice(0, 1450);

    const nextState = {
        idx: idx + 1,
        lastUrl: entry.lastUrl || ""
    };

    return { summary, hashtags, template, nextState, site, ctaCode };
}

async function bumpCycleState(env, profileId, nextState = {}, lastUrl = "", advance = true) {
    const state = await getCycleState(env);
    const prev = state[profileId] || { idx: 0, lastUrl: "" };
    state[profileId] = {
        idx: nextState.idx != null ?
            nextState.idx : advance ?
            prev.idx + 1 : prev.idx,
        lastUrl: lastUrl || prev.lastUrl || ""
    };
    await saveCycleState(env, state);
}

export async function getCycleStateForProfile(env, profileId = "") {
    const state = await getCycleState(env);
    if (!profileId) return state;
    const entry = state[profileId] || { idx: 0, lastUrl: "" };
    const nextTemplate = TEMPLATE_CYCLE[(entry.idx || 0) % TEMPLATE_CYCLE.length];
    return {...entry, nextTemplate };
}

export function buildQuickLinkLines(defaults = {}) {
    const quickLinks = [
        { label: "Reviews ►", url: defaults ? defaults.reviewLink : "" },
        { label: "Service Area ►", url: defaults ? defaults.serviceAreaLink : "" },
        { label: "Area Map ►", url: defaults ? defaults.areaMapLink : "" },
    ];
    return quickLinks
        .map((q) => {
            const val = String(q.url || "").trim();
            return /^https?:\/\//i.test(val) ? `${q.label} ${val}` : "";
        })
        .filter(Boolean);
}

export function insertQuickLinksBeforeHashtags(text = "", quickLines = []) {
    const base = typeof text === "string" ? text : "";
    const normalizedQuick = (quickLines || []).map((ql) => String(ql || "").trim()).filter(Boolean);
    if (!base && !normalizedQuick.length) return base;
    if (!normalizedQuick.length) return base;
    const lines = base ? base.split("\n") : [];
    const alreadyIncluded = normalizedQuick.every((ql) =>
        lines.some((line) => line.trim() === ql)
    );
    if (alreadyIncluded) return base;
    let hashtagStart = -1;
    for (let i = lines.length - 1; i >= 0; i--) {
        const trimmed = lines[i].trim();
        if (!trimmed) continue;
        if (trimmed.startsWith("#")) {
            hashtagStart = i;
            continue;
        }
        break;
    }
    const before = hashtagStart === -1 ? lines : lines.slice(0, hashtagStart);
    const hashtags = hashtagStart === -1 ? [] : lines.slice(hashtagStart);
    const result = [];
    if (before.length) {
        result.push(...before);
    }
    if (result.length && result[result.length - 1].trim()) {
        result.push("");
    }
    result.push(...normalizedQuick);
    if (hashtags.length) {
        if (normalizedQuick[normalizedQuick.length - 1]?.trim() && hashtags[0]?.trim()) {
            result.push("");
        }
        result.push(...hashtags);
    }
    return result.join("\n").replace(/\n{3,}/g, "\n\n").trim();
}

function hashString(str = "") {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
        hash = (hash << 5) - hash + str.charCodeAt(i);
        hash |= 0;
    }
    return hash;
}

function pickTemplateValue(list = [], profileId = "", seed = "") {
    if (!Array.isArray(list) || list.length === 0) return "";
    const hash = Math.abs(hashString(`${profileId}:${seed}`));
    return list[hash % list.length];
}

function formatTemplate(template = "", ctx = {}) {
    return template.replace(/\{(\w+)\}/g, (_match, key) => {
        return ctx[key] != null ? ctx[key] : "";
    });
}

const INTRO_TEMPLATES = [
    "{business} keeps {city} homes confident about {serviceLower}.",
    "{city} locals trust {business} when {serviceVerb} matters.",
    "From {city} to nearby neighbourhoods, {business} handles {serviceLower}.",
    "{business} delivers {serviceLower} that matches {city}'s pace."
];

const DIFFERENTIATOR_TEMPLATES = [
    "Every project includes {detailOne} and {detailTwo}.",
    "Expect clear timelines, {detailOne}, and {detailTwo}.",
    "{business} pairs skilled crews with {detailTwo}.",
    "Quick response, {detailOne}, and personal updates every step."
];

const DETAIL_OPTIONS_ONE = [
    "dust-controlled cleanup",
    "site protection",
    "respectful crews",
    "budget-friendly plans"
];

const DETAIL_OPTIONS_TWO = [
    "daily progress texts",
    "local permitting guidance",
    "finish-quality walk-throughs",
    "after-service check-ins"
];

const TEMPLATE_MESSAGES = {
    OFFER: [
        "Limited-time offer ready now—ask us to lock it in for {city}.",
        "{business} lined up a savings window for {serviceLower} this week."
    ],
    SOCIAL_PROOF: [
        "Neighbour referrals power most of our {serviceLower} work in {city}.",
        "Clients in {city} say {business} is their go-to for {serviceLower}."
    ],
    TIP: [
        "Pro tip: consistent {serviceLower} keeps {city} properties sharp.",
        "Reminder from {business}: schedule {serviceLower} before busy season hits {city}."
    ],
    default: [
        "Need a hand with {serviceLower}? {business} is ready.",
        "{business} handles last-minute {serviceLower} so you can relax."
    ]
};

export async function composeAiTemplatePost(env, profile, overrides = {}, basics = {}) {
    const tpl = await buildTemplatePost(env, profile, overrides, basics);
    const serviceKeyword = typeof overrides.serviceType === "string" ? overrides.serviceType.trim() : "";
    const aiProfile =
        serviceKeyword ?
        {
            ...profile,
            keywords: uniqueArray([serviceKeyword, ...(profile.keywords || [])])
        } :
        profile;
    const neighbourhood = pickNeighbourhood(aiProfile);
    let aiSummary = "";
    let aiHashtags = [];
    try {
        const gen = await aiGenerateSummaryAndHashtags(env, aiProfile, neighbourhood);
        aiSummary = (gen && gen.summary) || "";
        aiHashtags = (gen && Array.isArray(gen.hashtags) && gen.hashtags) || [];
    } catch (e) {
        console.error("AI summary error:", e);
    }

    const mergedHashtags = uniqueArray([...(tpl.hashtags || []), ...aiHashtags]).slice(
        0,
        12
    );

    const tplLines = (tpl.summary || "")
        .split(/\n+/)
        .map((l) => l.trim())
        .filter(Boolean);
    const linkLines = tplLines.filter((l) =>
        /^previous update:/i.test(l) || /^more info:/i.test(l)
    );
    const baseLines = tplLines.filter(
        (l) => !/^previous update:/i.test(l) && !/^more info:/i.test(l)
    );

    const summaryParts = [];
    if (baseLines.length) summaryParts.push(baseLines.join("\n"));
    if (aiSummary) summaryParts.push(aiSummary);
    if (linkLines.length) summaryParts.push(linkLines.join("\n"));

    let summary = summaryParts.filter(Boolean).join("\n\n");
    if (summary.length > 1500) summary = summary.slice(0, 1500);

    return {
        summary,
        hashtags: mergedHashtags,
        template: tpl.template,
        nextState: tpl.nextState,
        site: tpl.site,
        ctaCode: tpl.ctaCode,
        neighbourhood
    };
}
export async function enqueueScheduledBulk(env, payload) {
    const { profileId, images = [], startAt, cadenceDays = 1, body = {}, autoGenerateSummary = false } = payload || {};
    if (!profileId) throw new Error("Missing profileId");
    const sanitizedImages = (Array.isArray(images) ? images : [])
        .map((url) => ensureAbsoluteMediaUrl(env, url))
        .filter(Boolean);
    if (!sanitizedImages.length) throw new Error("No images provided");
    const normalizedBody = normalizeBodyMedia(env, body);
    const runStart = startAt ? new Date(startAt) : new Date(Date.now() + 3_600_000);
    if (isNaN(runStart.getTime())) throw new Error("Invalid startAt");
    const items = [];
    sanitizedImages.forEach((mediaUrl, idx) => {
        const runAt = new Date(runStart.getTime() + idx * cadenceDays * 86400000).toISOString();
        items.push({
            profileId,
            runAt,
            body: {...normalizedBody, mediaUrl, autoGenerateSummary },
        });
    });
    const existing = await getScheduledPosts(env);
    const combined = existing.concat(
        items.map((it) => ({
            id: crypto.randomUUID(),
            createdAt: new Date().toISOString(),
            ...it,
        }))
    );
    await saveScheduledPosts(env, combined);
    return { count: items.length };
}

export async function draftScheduledBulk(env, payload) {
    const { profileId, images = [], startAt, cadenceDays = 1, body = {}, autoGenerateSummary = false } = payload || {};
    if (!profileId) throw new Error("Missing profileId");
    const overrides = normalizeBodyMedia(env, body);
    const runStart = startAt ? new Date(startAt) : new Date(Date.now() + 3_600_000);
    if (isNaN(runStart.getTime())) throw new Error("Invalid startAt");
    const profileList = await getProfiles(env);
    const profile = profileList.find((p) => p.profileId === profileId);
    const mediaTopics = profile && profile.mediaTopics && typeof profile.mediaTopics === "object" ? profile.mediaTopics : {};
    const defaultTopic = profile ? getDefaultServiceTopic(profile) : null;
    const defaultTopicId = defaultTopic && defaultTopic.id ? defaultTopic.id : "";
    const normalizedImages = (Array.isArray(images) ? images : [])
        .map((entry) => {
            if (!entry) return null;
            if (typeof entry === "string") {
                const mediaUrl = ensureAbsoluteMediaUrl(env, entry);
                if (!mediaUrl) return null;
                const topicId = mediaTopics[mediaUrl] || defaultTopicId || "";
                return { mediaUrl, serviceTopicId: topicId };
            }
            if (typeof entry === "object") {
                const rawUrl = entry.mediaUrl || entry.url || entry.href || entry.source || "";
                const mediaUrl = ensureAbsoluteMediaUrl(env, rawUrl);
                if (!mediaUrl) return null;
                const topicId = String(entry.serviceTopicId || entry.topicId || mediaTopics[mediaUrl] || defaultTopicId || "").trim();
                return { mediaUrl, serviceTopicId: topicId };
            }
            return null;
        })
        .filter((entry) => entry && entry.mediaUrl);
    if (!normalizedImages.length) throw new Error("No images provided");

    let basicsCache = null;
    const loadBasics = async () => {
        if (!profile) return null;
        if (!basicsCache) {
            basicsCache = await fetchLocationBasics(env, profile);
        }
        return basicsCache;
    };

    const drafts = [];
    for (let idx = 0; idx < normalizedImages.length; idx++) {
        const { mediaUrl, serviceTopicId } = normalizedImages[idx];
        const runAt = new Date(runStart.getTime() + idx * cadenceDays * 86400000).toISOString();
        const overridesForDraft = {...overrides, mediaUrl };
        const topic = profile ?
            (findServiceTopic(profile, serviceTopicId) || defaultTopic) :
            null;
        if (topic && (topic.serviceType || topic.label)) {
            overridesForDraft.serviceType = topic.serviceType || topic.label;
        } else if (!topic && overrides.serviceType) {
            overridesForDraft.serviceType = overrides.serviceType;
        } else {
            delete overridesForDraft.serviceType;
        }

        const topicSummary = topic && topic.summary ? String(topic.summary).trim() : "";
        const topicHashtags = topic && Array.isArray(topic.hashtags) ? topic.hashtags : [];

        let postText = overridesForDraft.postText || "";
        if (!autoGenerateSummary && !postText && topicSummary) {
            postText = topicSummary;
        }
        let cta = overridesForDraft.cta || "";
        let linkUrl = overridesForDraft.linkUrl || "";
        if ((autoGenerateSummary || !postText) && profile) {
            const basics = await loadBasics();
            const built = await composeAiTemplatePost(env, profile, overridesForDraft || {}, basics || {});
            const mergedHashtags = uniqueArray([...(built.hashtags || []), ...topicHashtags]);
            const tagLine = mergedHashtags.join(" ");
            postText = (built.summary || "").trim();
            if (tagLine && postText.length + tagLine.length + 2 <= 1500) {
                postText += "\n\n" + tagLine;
            }
            cta = cta || built.ctaCode || "";
            linkUrl = linkUrl || built.site || "";
        } else if (topicHashtags.length) {
            const missing = topicHashtags.filter((tag) => tag && !postText.includes(tag));
            const tagLine = missing.join(" ");
            if (tagLine && postText.length + tagLine.length + 2 <= 1500) {
                postText = (postText || "").trim();
                postText += (postText ? "\n\n" : "") + tagLine;
            }
        }

        if (profile) {
            const quickLines = buildQuickLinkLines(profile.defaults);
            postText = insertQuickLinksBeforeHashtags(postText, quickLines);
        }
        if (postText.length > 1500) {
            postText = postText.slice(0, 1500);
        }

        const bodyPayload = {...overridesForDraft, mediaUrl };
        bodyPayload.postText = postText;
        bodyPayload.autoGenerateSummary = autoGenerateSummary;
        bodyPayload.cta = cta;
        bodyPayload.linkUrl = linkUrl;
        if (topic && topic.id) {
            bodyPayload.serviceTopicId = topic.id;
            bodyPayload.serviceTopicLabel = topic.label;
        } else if (serviceTopicId) {
            bodyPayload.serviceTopicId = serviceTopicId;
        }

        drafts.push({
            id: crypto.randomUUID(),
            runAt,
            profileId,
            body: bodyPayload,
        });
    }
    return drafts;
}

export async function appendPhotosToProfile(env, profileId, items = []) {
    const list = await getProfiles(env);
    const idx = list.findIndex((p) => p && p.profileId === profileId);
    if (idx === -1) throw new Error("Profile not found");
    const profile = list[idx];
    const pool = Array.isArray(profile.photoPool) ? profile.photoPool.slice() : [];

    const normalized = Array.isArray(items) ?
        items
        .map((it) => {
            if (!it) return null;
            const entry = typeof it === "string" ? {
                url: String(it).trim(),
                serviceType: "",
                serviceTopicId: "",
                captions: [],
                addedAt: new Date().toISOString()
            } : {...it };
            const fullUrl = ensureAbsoluteMediaUrl(env, entry.url || "");
            if (!/^https?:\/\//i.test(fullUrl)) return null;
            return {
                url: fullUrl,
                serviceType: String(entry.serviceType || ""),
                serviceTopicId: String(entry.serviceTopicId || ""),
                captions: Array.isArray(entry.captions) ? entry.captions.slice(0, 5) : [],
                addedAt: entry.addedAt || new Date().toISOString()
            };
        })
        .filter(Boolean) : [];

    pool.push(...normalized);
    const trimmed = pool.slice(-200); // cap to avoid unbounded growth
    profile.photoPool = trimmed;
    list[idx] = profile;
    await saveProfiles(env, list);
    return profile;
}

export async function fetchLocationBasics(env, profile) {
    const out = { websiteUri: "", primaryPhone: "", mapsUri: "", placeReviewUri: "" };
    if (!profile) return out;
    const locationId = String(profile.locationId || "");
    if (!locationId) return out;

    const normalizeLocationPath = (id) => {
        const str = String(id || "").trim();
        if (!str) return "";
        const match = str.match(/locations\/(.+)$/i);
        if (match) {
            return `locations/${match[1]}`;
        }
        return `locations/${str}`;
    };

    const locationPath = normalizeLocationPath(locationId);
    if (!locationPath) return out;

    const base = "https://mybusinessbusinessinformation.googleapis.com/v1";
    const readMask = "websiteUri,phoneNumbers,metadata";
    const url = `${base}/${locationPath}?readMask=${readMask}`;

    const parseBasics = (data) => {
        const next = {...out };
        next.websiteUri = data.websiteUri || "";
        if (data.phoneNumbers && data.phoneNumbers.primaryPhone) {
            next.primaryPhone = String(data.phoneNumbers.primaryPhone);
        }
        const meta = data.metadata || {};
        next.mapsUri = meta.mapsUri || "";
        next.placeReviewUri = meta.placeReviewUri || "";
        return next;
    };

    try {
        const resp = await callBusinessProfileAPI(env, "GET", url);
        const data = resp && resp.data ? resp.data : {};
        return parseBasics(data);
    } catch (e) {
        const msg = String(e && e.message ? e.message : e);
        const is404 = /404/.test(msg);
        const logFn = is404 ? console.warn : console.error;
        logFn("fetchLocationBasics error:", msg);
        return out;
    }
}

// CTA helpers
const CTA_MAP = {
    LEARN_MORE: { actionType: "LEARN_MORE", needsUrl: true },
    BOOK: { actionType: "BOOK", needsUrl: true },
    ORDER: { actionType: "ORDER", needsUrl: true },
    SHOP: { actionType: "SHOP", needsUrl: true },
    SIGN_UP: { actionType: "SIGN_UP", needsUrl: true },
    CALL_NOW: { actionType: "CALL", needsUrl: false }
};

function hasHttp(url) {
    return typeof url === "string" && /^https?:\/\//i.test(url);
}

export function buildCallToAction(ctaCode, linkUrl, basics, profile, phoneOverride = "") {
    const code = ctaCode || "LEARN_MORE";

    if (code === "CALL" || code === "CALL_NOW") {
        const phoneFromLink =
            typeof linkUrl === "string" && linkUrl.toLowerCase().startsWith("tel:") ?
            linkUrl.replace(/^tel:/i, "").trim() :
            "";
        const rawPhone =
            phoneFromLink ||
            phoneOverride ||
            (profile && profile.defaults && profile.defaults.phone) ||
            (basics && basics.primaryPhone) ||
            "";
        const phone = String(rawPhone || "").trim().replace(/^tel:/i, "");
        if (phone) {
            // GBP expects only the actionType for CALL; do not send url/phoneNumber fields.
            return { actionType: "CALL" };
        }
        // No phone: return null so caller can decide to block/post error instead of silently switching CTA
        return null;
    }

    const spec = CTA_MAP[code] || null;
    if (!spec) return null;

    let url = "";
    if (hasHttp(linkUrl)) {
        url = linkUrl;
    } else if (spec.needsUrl) {
        url = (basics && basics.websiteUri) || (profile && profile.landingUrl) || "";
    }

    if (spec.needsUrl && !hasHttp(url)) return null;
    const payload = { actionType: spec.actionType };
    if (spec.needsUrl) payload.url = url;
    return payload;
}

// Posts history
export async function appendPostHistory(env, payload) {
    const arr = (await getJson(env, "posts-history", [])) || [];
    const profile = payload.profile || {};
    const usedImageUrl = payload.usedImageUrl || "";
    const rec = {
        id: String(Date.now()) +
            "-" +
            Math.random().toString(36).slice(2, 8),
        createdAt: payload.createdAt || new Date().toISOString(),
        locationId: profile.locationId || "",
        profileId: profile.profileId || "",
        profileName: profile.businessName || "",
        summary: payload.summary || "",
        mediaCount: usedImageUrl ? 1 : 0,
        usedImage: !!usedImageUrl,
        cta: payload.cta || "",
        linkUrl: payload.linkUrl || "",
        postedUrl: payload.postedUrl || "",
        status: payload.status || "PENDING",
        gmbPostId: payload.gmbPostId || "",
        overlayUrl: payload.overlayUrl || ""
    };
    arr.push(rec);
    const trimmed = arr.length > 1000 ? arr.slice(-1000) : arr;
    await setJson(env, "posts-history", trimmed);
    return rec;
}

export async function getPostsHistory(env, profileId, limit = 50) {
    let arr = (await getJson(env, "posts-history", [])) || [];
    if (!Array.isArray(arr)) arr = [];
    if (profileId) {
        arr = arr.filter((x) => x && x.profileId === profileId);
    }
    if (limit && arr.length > limit) {
        arr = arr.slice(-limit);
    }
    return arr;
}

function isHttpsImage(url) {
    return typeof url === "string" && /^https:\/\/.+\.(png|jpe?g|webp)$/i.test(url);
}

function getMediaBase(env) {
    const base =
        env &&
        (env.PUBLIC_BASE_URL ||
            env.PUBLIC_MEDIA_BASE ||
            env.MEDIA_BASE_URL ||
            env.BACKEND_BASE_URL ||
            DEFAULT_MEDIA_BASE);
    const cleaned = base ? String(base).replace(/\/+$/, "") : "";
    // debug log (temporary): show which base is used when resolving media
    console.log("getMediaBase ->", { candidateBase: base, cleanedBase: cleaned });
    return cleaned;
}

export function ensureAbsoluteMediaUrl(env, value, options = {}) {
    const raw = String(value || "").trim();
    if (!raw) return "";
    if (/^https?:\/\//i.test(raw)) {
        // already absolute
        return raw;
    }
    const path = raw.startsWith("/") ? raw : "/" + raw;
    const base = getMediaBase(env);
    const result = base ? base + path : (options && options.allowRelativeFallback === false ? raw : path);
    // debug log (temporary): show input -> output so we can spot where prefix is missing
    console.log("ensureAbsoluteMediaUrl ->", {
        rawInput: raw,
        path,
        base,
        result,
        allowRelativeFallback: !!(options && options.allowRelativeFallback)
    });
    return result;
}

function normalizeBodyMedia(env, body = {}) {
    if (!body || typeof body !== "object") return {};
    const out = {...body };
    if (out.mediaUrl) out.mediaUrl = ensureAbsoluteMediaUrl(env, out.mediaUrl);
    if (out.overlayUrl) out.overlayUrl = ensureAbsoluteMediaUrl(env, out.overlayUrl);
    return out;
}

function resolveMediaUrl(env, mediaUrlRaw) {
    const raw = String(mediaUrlRaw || "").trim();
    if (!raw) throw new Error("Missing mediaUrl");
    const url = ensureAbsoluteMediaUrl(env, raw, { allowRelativeFallback: false });
    if (/^https?:\/\//i.test(url)) return url;
    throw new Error("mediaUrl must be https://... or set PUBLIC_BASE_URL/PUBLIC_MEDIA_BASE to prefix /uploads files");
}

function resolveMediaUrlForPost(env, mediaUrlRaw) {
    const raw = String(mediaUrlRaw || "").trim();
    if (!raw) return "";
    try {
        const full = ensureAbsoluteMediaUrl(env, raw);
        return isHttpsImage(full) ? full : "";
    } catch (err) {
        console.warn(
            "resolveMediaUrlForPost failed:",
            raw,
            err && err.message ? err.message : err
        );
        return "";
    }
}

async function callMediaApi(env, path, method = "GET", body = null) {
    const token = await getAccessToken(env);
    const hosts = [
        "https://mybusiness.googleapis.com/v4/",
        "https://mybusinesscontent.googleapis.com/v1/",
        "https://businessprofile.googleapis.com/v1/"
    ];
    let lastErr = null;
    for (const host of hosts) {
        const url = host.replace(/\/+$/, "/") + path.replace(/^\/+/, "");
        const res = await fetch(url, {
            method,
            headers: {
                Authorization: `Bearer ${token}`,
                "Content-Type": "application/json"
            },
            body: body ? JSON.stringify(body) : undefined
        });
        if (res.ok) {
            const data = await res.json().catch(() => ({}));
            return data;
        }
        const text = await res.text().catch(() => res.statusText);
        lastErr = new Error(text || `Google API ${res.status}`);
        // If this host returns 404, try the next host
        if (res.status === 404) continue;
        throw lastErr;
    }
    if (lastErr) throw lastErr;
    throw new Error("Google API call failed");
}

async function callMediaApiWithFallback(env, paths = [], method = "GET", body = null) {
    const list = Array.isArray(paths) ? paths : [paths];
    let lastErr = null;
    for (const p of list) {
        try {
            const res = await callMediaApi(env, p, method, body);
            return res;
        } catch (e) {
            const msg = e && e.message ? String(e.message) : "";
            lastErr = new Error(`Media API failed for ${p}: ${msg}`);
            // Continue to next path only if 404/Not found
            if (/404/.test(msg)) {
                continue;
            }
            throw lastErr;
        }
    }
    if (lastErr) throw lastErr;
    throw new Error("No paths provided for media API");
}

async function verifyLocationExists(env, profile) {
    if (!profile || !profile.locationId) {
        throw new Error("Profile missing locationId");
    }
    const url = `https://mybusinessbusinessinformation.googleapis.com/v1/locations/${profile.locationId}?readMask=name`;
    const res = await callBusinessProfileApi(env, url, { method: "GET" }).catch(async(e) => {
        const msg = e && e.message ? String(e.message) : "";
        if (/404/.test(msg)) {
            throw new Error(
                `Google location not found for locationId=${profile.locationId}. Refresh locations (Diagnostics → List locations) or resync profiles.`
            );
        }
        throw e;
    });
    return res;
}

export async function uploadPhotoToGmb(env, profile, body = {}) {
    if (!profile) throw new Error("Profile required for photo upload");
    if (!profile.locationId) throw new Error("Profile missing locationId");
    await verifyLocationExists(env, profile);
    const mediaUrl = resolveMediaUrl(env, body.mediaUrl || profile.defaults?.mediaUrl || "");
    const parentPaths = [
        `locations/${profile.locationId}/media`,
        profile.accountId ? `accounts/${profile.accountId}/locations/${profile.locationId}/media` : null
    ].filter(Boolean);
    const payload = {
        mediaFormat: "PHOTO",
        sourceUrl: mediaUrl,
        locationAssociation: {
            category: "ADDITIONAL"
        }
    };
    if (body.caption) {
        payload.description = String(body.caption).slice(0, 1500);
    }
    return callMediaApiWithFallback(env, parentPaths, "POST", payload);
}

export async function fetchLatestMedia(env, profileId, pageSize = 10) {
    if (!profileId) throw new Error("Missing profileId");
    const profiles = await getProfiles(env);
    const profile = profiles.find((p) => p && p.profileId === profileId);
    if (!profile) throw new Error("Profile not found");
    if (!profile.locationId) throw new Error("Profile missing locationId");
    await verifyLocationExists(env, profile);
    const paths = [
        `locations/${profile.locationId}/media?pageSize=${pageSize}`,
        profile.accountId ?
        `accounts/${profile.accountId}/locations/${profile.locationId}/media?pageSize=${pageSize}` :
        null
    ].filter(Boolean);
    const data = await callMediaApiWithFallback(env, paths, "GET", null);
    const items = (data && data.mediaItems) || [];
    return items;
}

export async function fetchMediaPaged(env, profileId, pageSize = 20, pages = 3) {
    if (!profileId) throw new Error("Missing profileId");
    const profiles = await getProfiles(env);
    const profile = profiles.find((p) => p && p.profileId === profileId);
    if (!profile) throw new Error("Profile not found");
    if (!profile.locationId) throw new Error("Profile missing locationId");
    await verifyLocationExists(env, profile);

    const all = [];
    let pageToken = "";
    let remaining = Math.max(1, Math.min(pages, 10)); // cap pages to avoid hammering
    while (remaining > 0) {
        const tokenPart = pageToken ? `&pageToken=${encodeURIComponent(pageToken)}` : "";
        const paths = [
            `locations/${profile.locationId}/media?pageSize=${pageSize}${tokenPart}`,
            profile.accountId ?
            `accounts/${profile.accountId}/locations/${profile.locationId}/media?pageSize=${pageSize}${tokenPart}` :
            null
        ].filter(Boolean);
        const data = await callMediaApiWithFallback(env, paths, "GET", null);
        const items = (data && data.mediaItems) || [];
        all.push(...items);
        pageToken = data.nextPageToken || "";
        if (!pageToken) break;
        remaining -= 1;
    }
    return { items: all };
}

export async function postToGmb(env, body) {
    const profileId = (body && body.profileId) || "";
    if (!profileId) throw new Error("Missing profileId");

    const profiles = await getProfiles(env);
    const profile = profiles.find((p) => p && p.profileId === profileId);
    if (!profile) throw new Error("Profile not found");

    const basics = await fetchLocationBasics(env, profile);

    let templateState = null;
    let summary = (body && body.postText) ? String(body.postText).trim() : "";
    let hashtags = [];
    let ctaFromTemplate = null;
    let linkOverride = null;

    if (!summary) {
        const built = await composeAiTemplatePost(env, profile, body || {}, basics);
        summary = built.summary || "";
        hashtags = built.hashtags || [];
        templateState = { idx: built.nextState ? built.nextState.idx : null };
        ctaFromTemplate = built.ctaCode || null;
        linkOverride = built.site || null;
    }

    const defaults = (profile && profile.defaults) || {};
    const quickLines = buildQuickLinkLines(defaults);
    summary = insertQuickLinksBeforeHashtags(summary, quickLines);

    if (hashtags.length) {
        const spaceLeft = 1450 - summary.length;
        if (spaceLeft > 20) {
            const tagLine = safeJoinHashtags(hashtags, spaceLeft);
            if (tagLine && summary.length + 2 + tagLine.length <= 1450) {
                summary += "\n\n" + tagLine;
            }
        }
    }

    if (summary.length > 1500) summary = summary.slice(0, 1500);

    const ctaCode = (body && body.cta) || ctaFromTemplate || defaults.cta || "LEARN_MORE";
    let linkUrl =
        (body && body.linkUrl) ||
        linkOverride ||
        defaults.linkUrl ||
        basics.websiteUri ||
        profile.landingUrl ||
        "";
    const overlayUsed = ensureAbsoluteMediaUrl(env, (body && body.overlayUrl) || "");
    const phoneOverride = (body && body.phone) || defaults.phone || "";
    const providedLinkUrl = linkUrl;
    const siteCandidate = basics.websiteUri || profile.landingUrl || "";
    let mediaUrlRaw = ensureAbsoluteMediaUrl(env, (body && body.mediaUrl) || defaults.mediaUrl || "");
    let usedFromPool = false;

    const ctaObj = buildCallToAction(ctaCode, linkUrl, basics, profile, phoneOverride);
    // Safety: CALL should never include a url to satisfy GBP validation
    const safeCta =
        ctaObj && ctaObj.actionType === "CALL" ? { actionType: "CALL" } :
        ctaObj;
    console.log("CTA_DEBUG", {
        profileId,
        ctaCode,
        linkUrl,
        phoneOverride,
        defaultsPhone: defaults.phone || "",
        basicsPhone: basics.primaryPhone || "",
        landingUrl: profile.landingUrl || "",
        resolvedCta: safeCta
    });

    if ((ctaCode === "CALL" || ctaCode === "CALL_NOW") && !safeCta) {
        throw new Error("Call now CTA requires a phone on the profile (or tel:+ link).");
    }

    const parent =
        "accounts/" + profile.accountId + "/locations/" + profile.locationId;
    const url = "https://mybusiness.googleapis.com/v4/" + parent + "/localPosts";

    const allowedTypes = ["STANDARD", "EVENT", "OFFER", "ALERT"];
    let topicType = (body && body.topicType) || "STANDARD";
    if (!allowedTypes.includes(topicType)) topicType = "STANDARD";

    const payload = {
        languageCode: "en",
        topicType,
        summary
    };
    if (safeCta) {
        payload.callToAction = safeCta;
    }
    const linkUsed = safeCta && safeCta.url ? safeCta.url : linkUrl;

    // EVENT details
    if (topicType === "EVENT") {
        const eventTitle = body && body.eventTitle ? String(body.eventTitle) : "";
        const startStr = body && body.eventStart ? String(body.eventStart) : "";
        const endStr = body && body.eventEnd ? String(body.eventEnd) : startStr;

        function parseYmd(s) {
            const parts = s.split("-");
            if (parts.length !== 3) return null;
            const y = parseInt(parts[0], 10);
            const m = parseInt(parts[1], 10);
            const d = parseInt(parts[2], 10);
            if (!y || !m || !d) return null;
            return { year: y, month: m, day: d };
        }

        const startDate = parseYmd(startStr);
        const endDate = parseYmd(endStr);

        if (startDate && endDate) {
            payload.event = {
                title: eventTitle || (profile.businessName + " event"),
                schedule: {
                    startDate,
                    endDate
                }
            };
        } else {
            console.warn("EVENT topicType but missing/invalid dates, falling back to STANDARD");
            payload.topicType = "STANDARD";
        }
    }

    // OFFER details
    if (topicType === "OFFER") {
        const offerTitle = body && body.offerTitle ? String(body.offerTitle) : "";
        const coupon = body && body.offerCoupon ? String(body.offerCoupon) : "";
        const offerRedeemUrl =
            (body && body.offerRedeemUrl && String(body.offerRedeemUrl)) ||
            providedLinkUrl ||
            siteCandidate ||
            "";

        payload.offer = {
            summary: offerTitle || undefined,
            couponCode: coupon || undefined,
            redemptionUrl: offerRedeemUrl || undefined
                // Additional fields available per GBP API
        };
    }

    let usedImageUrl = "";
    if (!mediaUrlRaw && Array.isArray(profile.photoPool) && profile.photoPool.length) {
        const candidate = profile.photoPool[0];
        const url = candidate && typeof candidate === "object" ? candidate.url : candidate;
        const serviceType = candidate && typeof candidate === "object" ? candidate.serviceType : "";
        const captions = candidate && typeof candidate === "object" ? candidate.captions : [];
        const normalizedPoolUrl = ensureAbsoluteMediaUrl(env, url || "");
        if (isHttpsImage(normalizedPoolUrl)) {
            mediaUrlRaw = normalizedPoolUrl;
            usedFromPool = true;
            // If a caption exists, prefer the first as post text when none supplied
            if (!summary && captions && captions.length) {
                summary = captions[0];
            }
            if (!postType && serviceType) {
                // no-op; could map service type to topic if needed
            }
        }
    }

    const resolvedMediaUrl = resolveMediaUrlForPost(env, mediaUrlRaw);
    if (resolvedMediaUrl) {
        payload.media = [{ mediaFormat: "PHOTO", sourceUrl: resolvedMediaUrl }];
        usedImageUrl = resolvedMediaUrl;
    }

    const extractPostedUrl = (result) => {
        const data = result && result.data;
        return (data && (data.searchUrl || data.name)) || "";
    };

    try {
        const result = await callBusinessProfileAPI(env, "POST", url, payload);
        const postedUrl = extractPostedUrl(result);
        await appendPostHistory(env, {
            profile,
            summary,
            usedImageUrl,
            linkUrl: linkUsed,
            cta: (ctaObj && ctaObj.actionType) || "",
            status: "POSTED",
            gmbPostId: result && result.data && result.data.name,
            postedUrl,
            overlayUrl: overlayUsed
        });
        if (templateState) {
            await bumpCycleState(env, profileId, templateState, postedUrl, true);
        } else if (postedUrl) {
            await bumpCycleState(env, profileId, {}, postedUrl, false);
        }
        if (usedFromPool && usedImageUrl) {
            try {
                const list = await getProfiles(env);
                const idx = list.findIndex((p) => p && p.profileId === profileId);
                if (idx !== -1) {
                    const updated = Array.isArray(list[idx].photoPool) ?
                        list[idx].photoPool.slice(1) : [];
                    list[idx].photoPool = updated;
                    await saveProfiles(env, list);
                }
            } catch (poolErr) {
                console.error("Failed to trim photoPool after post:", poolErr);
            }
        }
        return {
            data: result.data,
            usedImage: usedImageUrl || null,
            ctaUsed: ctaObj || null,
            ctaStripped: false,
            firstError: null,
            postedUrl
        };
    } catch (err) {
        const msg = String(err && err.message ? err.message : err);
        if (/INVALID_ARGUMENT/i.test(msg) && payload.media) {
            // retry without media
            delete payload.media;
            usedImageUrl = "";
            const result = await callBusinessProfileAPI(env, "POST", url, payload);
            const postedUrl = extractPostedUrl(result);
            await appendPostHistory(env, {
                profile,
                summary,
                linkUrl: linkUsed,
                usedImageUrl: "",
                cta: (ctaObj && ctaObj.actionType) || "",
                status: "POSTED",
                gmbPostId: result && result.data && result.data.name,
                postedUrl,
                overlayUrl: overlayUsed
            });
            return {
                data: result.data,
                usedImage: null,
                ctaUsed: ctaObj || null,
                ctaStripped: false,
                firstError: msg,
                postedUrl
            };
        }

        await appendPostHistory(env, {
            profile,
            summary,
            usedImageUrl: "",
            linkUrl: linkUsed,
            cta: (ctaObj && ctaObj.actionType) || "",
            status: "FAILED",
            gmbPostId: "",
            overlayUrl: overlayUsed
        });
        throw err;
    }
}

// Scheduler helpers
export async function getSchedulerConfig(env) {
    const raw = (await getJson(env, "schedulerConfig", null)) || null;
    const cfg = {...DEFAULT_SCHED, ...(raw || {}) };
    if (!cfg.defaultTime) cfg.defaultTime = "10:00";
    if (!cfg.tickSeconds || typeof cfg.tickSeconds !== "number") {
        cfg.tickSeconds = DEFAULT_SCHED.tickSeconds;
    }
    if (!cfg.perProfileTimes || typeof cfg.perProfileTimes !== "object") {
        cfg.perProfileTimes = {};
    }
    if (!cfg.perProfileIntervalDays || typeof cfg.perProfileIntervalDays !== "object") {
        cfg.perProfileIntervalDays = {};
    }
    if (!cfg.defaultIntervalDays || typeof cfg.defaultIntervalDays !== "number") {
        cfg.defaultIntervalDays = DEFAULT_SCHED.defaultIntervalDays;
    }
    if (!cfg.perProfileCadence || typeof cfg.perProfileCadence !== "object") {
        cfg.perProfileCadence = {};
    }
    if (!cfg.defaultCadence || typeof cfg.defaultCadence !== "string") {
        cfg.defaultCadence = DEFAULT_SCHED.defaultCadence;
    }
    return cfg;
}

export async function setSchedulerConfig(env, partial) {
    const old = await getSchedulerConfig(env);
    const cfg = {...old };
    if (typeof partial.enabled === "boolean") cfg.enabled = partial.enabled;
    if (typeof partial.defaultTime === "string") cfg.defaultTime = partial.defaultTime;
    if (typeof partial.tickSeconds === "number") cfg.tickSeconds = partial.tickSeconds;
    if (typeof partial.defaultIntervalDays === "number") {
        cfg.defaultIntervalDays = partial.defaultIntervalDays;
    }
    if (typeof partial.defaultCadence === "string") {
        cfg.defaultCadence = partial.defaultCadence;
    }

    const ppt = partial.perProfileTimes || {};
    if (ppt && typeof ppt === "object") {
        const cleaned = {};
        for (const [k, vRaw] of Object.entries(ppt)) {
            const v = String(vRaw || "");
            if (/^\d{2}:\d{2}$/.test(v)) cleaned[k] = v;
        }
        cfg.perProfileTimes = cleaned;
    }

    const ppi = partial.perProfileIntervalDays || {};
    if (ppi && typeof ppi === "object") {
        const cleaned = {};
        for (const [k, vRaw] of Object.entries(ppi)) {
            const vNum = parseInt(vRaw, 10);
            if (vNum && vNum > 0 && vNum < 15) cleaned[k] = vNum;
        }
        cfg.perProfileIntervalDays = cleaned;
    }

    const ppc = partial.perProfileCadence || {};
    if (ppc && typeof ppc === "object") {
        const cleaned = {};
        for (const [k, vRaw] of Object.entries(ppc)) {
            const val = String(vRaw || "").toUpperCase();
            if (["DAILY1", "DAILY2", "DAILY3", "WEEKLY1"].includes(val)) cleaned[k] = val;
        }
        cfg.perProfileCadence = cleaned;
    }

    await setJson(env, "schedulerConfig", cfg);
    return cfg;
}

export async function getSchedulerStatus(env) {
    const cfg = await getSchedulerConfig(env);
    const profiles = await getProfiles(env);
    const lastRunMap = (await getJson(env, "schedulerLastRun", {})) || {};
    const todayISO = new Date().toISOString().slice(0, 10);

    const items = profiles.map((p) => {
        const hhmm =
            (cfg.perProfileTimes && cfg.perProfileTimes[p.profileId]) ||
            cfg.defaultTime ||
            "10:00";
        const lastRunRaw = lastRunMap[p.profileId] || null;
        const intervalDays =
            (cfg.perProfileIntervalDays && cfg.perProfileIntervalDays[p.profileId]) ||
            cfg.defaultIntervalDays ||
            1;
        const cadence =
            (cfg.perProfileCadence && cfg.perProfileCadence[p.profileId]) ||
            cfg.defaultCadence ||
            "DAILY1";
        const lastDate = lastRunRaw && lastRunRaw.date ?
            new Date(lastRunRaw.date) :
            lastRunRaw ?
            new Date(lastRunRaw) :
            null;
        const nextDate = lastDate ?
            new Date(new Date(lastDate).setDate(lastDate.getDate() + intervalDays)) :
            null;
        const active = isProfileActive(p);
        const willRunToday = !!cfg.enabled && active;
        return {
            profileId: p.profileId,
            businessName: p.businessName || "",
            scheduledTime: hhmm,
            lastRunISODate: lastDate ? lastDate.toISOString().slice(0, 10) : null,
            intervalDays,
            cadence,
            photoQueueSize: Array.isArray(p.photoPool) ? p.photoPool.length : 0,
            nextEligibleISODate: nextDate ? nextDate.toISOString().slice(0, 10) : todayISO,
            willRunToday,
            disabled: !active
        };
    });

    return {
        enabled: cfg.enabled,
        defaultTime: cfg.defaultTime,
        tickSeconds: cfg.tickSeconds,
        defaultIntervalDays: cfg.defaultIntervalDays,
        defaultCadence: cfg.defaultCadence,
        todayISO,
        profiles: items
    };
}

export async function runSchedulerOnce(env) {
    const profiles = await getProfiles(env);
    const results = [];
    for (const p of profiles) {
        if (!isProfileActive(p)) continue;
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
    return { ok: true, results };
}

export async function runSchedulerNow(env, profileId) {
    const r = await postToGmb(env, { profileId });
    return r;
}

export async function enqueueScheduledPost(env, payload) {
    const id = crypto.randomUUID();
    const runAt = new Date(payload.runAt).toISOString();
    const item = {
        id,
        runAt,
        createdAt: new Date().toISOString(),
        profileId: payload.profileId,
        body: normalizeBodyMedia(env, payload.body || {}),
        status: "QUEUED"
    };
    await upsertScheduledRow(env, item);
    return item;
}

export async function updateScheduledPost(env, id, updates) {
    const list = await getAllScheduledPosts(env);
    const current = list.find((it) => it && it.id === id);
    if (!current) throw new Error("Scheduled post not found");
    const runAt = updates.runAt ? new Date(updates.runAt).toISOString() : current.runAt;
    const updated = {
        ...current,
        runAt,
        body: updates.body ? normalizeBodyMedia(env, updates.body) : current.body,
        status: current.status || "QUEUED"
    };
    await upsertScheduledRow(env, updated);
    return updated;
}

export async function deleteScheduledPost(env, id) {
    await deleteScheduledRow(env, id);
}

async function getAutoSchedulerQueue(env) {
    const raw = (await getJson(env, AUTO_QUEUE_KEY, [])) || [];
    if (!Array.isArray(raw)) return [];
    return raw
        .map((item) => {
            if (!item || typeof item !== "object") return null;
            if (!item.profileId) return null;
            const date = item.date || "";
            const hhmm = item.hhmm || "";
            return {
                profileId: item.profileId,
                date,
                hhmm,
                enqueuedAt: item.enqueuedAt || new Date().toISOString()
            };
        })
        .filter(Boolean)
        .slice(0, 200);
}

async function saveAutoSchedulerQueue(env, queue) {
    await setJson(env, AUTO_QUEUE_KEY, Array.isArray(queue) ? queue : []);
}

function autoQueueKey(entry) {
    return `${entry.profileId || ""}:${entry.date || ""}:${entry.hhmm || ""}`;
}

export async function scheduledTick(env) {
    // Process explicit scheduled posts first
    const scheduled = await getAllScheduledPosts(env);
    const profiles = await getProfiles(env);
    const nowMs = Date.now();

    // Process scheduled photos (photo-only queue)
    const scheduledPhotos = await getAllScheduledPhotos(env);
    for (const item of scheduledPhotos) {
        if (item.status && item.status !== "QUEUED") continue;
        const due = item && item.runAt ? new Date(item.runAt).getTime() : 0;
        if (due && due <= nowMs && item.profileId) {
            try {
                const profile = profiles.find((p) => p && p.profileId === item.profileId);
                if (!profile) throw new Error("Profile not found for scheduled photo");
                const photoMediaUrl = ensureAbsoluteMediaUrl(env, item.body?.mediaUrl || "");
                if (!photoMediaUrl) throw new Error("Scheduled photo missing mediaUrl");
                const body = {
                    profileId: item.profileId,
                    mediaUrl: photoMediaUrl,
                    caption: item.body?.caption || ""
                };
                await uploadPhotoToGmb(env, profile, body);
                console.log("[scheduled-photo] Uploaded photo to GBP library", item.id, "for", item.profileId);
                await markPhotoPosted(env, item.id);
            } catch (e) {
                console.error("[scheduled-photo] Failed for", item.profileId, item.id, String(e && e.message ? e.message : e));
                await markPhotoFailed(env, item.id, e && e.message ? e.message : String(e));
            }
        }
    }

    for (const item of scheduled) {
        if (item.status && item.status !== "QUEUED") continue;
        const due = item && item.runAt ? new Date(item.runAt).getTime() : 0;
        if (due && due <= nowMs && item.profileId) {
            try {
                const profile = profiles.find((p) => p.profileId === item.profileId);
                const normalizedBody = normalizeBodyMedia(env, item.body || {});
                const body = { profileId: item.profileId, ...normalizedBody };
                const res = await postToGmb(env, body);
                const postedUrl = (res && res.postedUrl) || "";
                console.log("[scheduled-post] Posted queued item", item.id, "for", item.profileId);
                await markScheduledPosted(env, item.id, postedUrl);
            } catch (e) {
                console.error("[scheduled-post] Failed for", item.profileId, item.id, String(e && e.message ? e.message : e));
            }
        }
    }

    const cfg = await getSchedulerConfig(env);
    const autoSchedulerEnabled = env.AUTO_SCHEDULER_ENABLED === "true";
    if (!autoSchedulerEnabled || !cfg.enabled) {
        return;
    }

    let lastRunMap = (await getJson(env, "schedulerLastRun", {})) || {};
    if (!lastRunMap || typeof lastRunMap !== "object") lastRunMap = {};
    let autoQueue = await getAutoSchedulerQueue(env);
    const existingQueueKeys = new Set(autoQueue.map(autoQueueKey));
    let queueModified = false;

    const now = new Date();
    const hh = String(now.getHours()).padStart(2, "0");
    const mm = String(now.getMinutes()).padStart(2, "0");
    const hhmm = hh + ":" + mm;
    const today = now.toISOString().slice(0, 10);

    function buildSlots(base, cadence) {
        const toMinutes = (s) => {
            const [h, m] = s.split(":").map((x) => parseInt(x, 10));
            return h * 60 + m;
        };
        const toHHMM = (mins) => {
            const m = ((mins % 1440) + 1440) % 1440;
            const h = Math.floor(m / 60);
            const mm2 = m % 60;
            return String(h).padStart(2, "0") + ":" + String(mm2).padStart(2, "0");
        };
        const baseM = toMinutes(base || "10:00");
        switch (cadence) {
            case "DAILY2":
                return [baseM, baseM + 360].map(toHHMM); // 6h apart
            case "DAILY3":
                return [baseM, baseM + 240, baseM + 480].map(toHHMM); // every 4h
            case "WEEKLY1":
                return [baseM];
            case "DAILY1":
            default:
                return [baseM].map(toHHMM);
        }
    }

    function normalizeLast(entry) {
        if (!entry) return { date: "", times: {} };
        if (typeof entry === "string") return { date: entry, times: {} };
        if (typeof entry === "object") {
            return {
                date: entry.date || "",
                times: entry.times || {}
            };
        }
        return { date: "", times: {} };
    }

    for (const p of profiles) {
        if (!isProfileActive(p)) continue;
        const target =
            (cfg.perProfileTimes && cfg.perProfileTimes[p.profileId]) ||
            cfg.defaultTime ||
            "10:00";
        const cadence =
            (cfg.perProfileCadence && cfg.perProfileCadence[p.profileId]) ||
            cfg.defaultCadence ||
            "DAILY1";
        const interval =
            (cfg.perProfileIntervalDays && cfg.perProfileIntervalDays[p.profileId]) ||
            (cadence === "WEEKLY1" ? 7 : cfg.defaultIntervalDays || 1) ||
            1;

        const slots = buildSlots(target, cadence);
        if (!slots.includes(hhmm)) continue;

        const last = normalizeLast(lastRunMap[p.profileId]);
        if (last.date) {
            const lastDate = new Date(last.date);
            const diff =
                Math.floor(
                    (new Date(today).getTime() - new Date(lastDate).getTime()) / 86400000
                ) || 0;
            if (cadence === "WEEKLY1" && diff < 7) continue;
            if (diff < interval && last.date === today && last.times && last.times[hhmm]) {
                continue;
            }
        }

        const key = `${p.profileId}:${today}:${hhmm}`;
        if (!existingQueueKeys.has(key)) {
            autoQueue.push({
                profileId: p.profileId,
                date: today,
                hhmm,
                enqueuedAt: new Date().toISOString()
            });
            existingQueueKeys.add(key);
            queueModified = true;
        }
    }

    if (queueModified) {
        await saveAutoSchedulerQueue(env, autoQueue);
    }

    const perTickLimitRaw = env.AUTO_SCHEDULER_PER_TICK;
    const perTickLimit =
        typeof perTickLimitRaw === "string" ?
        parseInt(perTickLimitRaw, 10) || DEFAULT_AUTO_PER_TICK :
        DEFAULT_AUTO_PER_TICK;
    const toProcess = autoQueue.slice(0, Math.max(1, perTickLimit));
    autoQueue = autoQueue.slice(toProcess.length);

    for (const entry of toProcess) {
        const profile = profiles.find((p) => p && p.profileId === entry.profileId);
        if (!profile) {
            console.warn("[scheduler] Queue entry missing profile", entry.profileId);
            continue;
        }
        try {
            await postToGmb(env, { profileId: entry.profileId });
            const last = normalizeLast(lastRunMap[entry.profileId]);
            const timesMap = last.times || {};
            const marker = entry.hhmm || hhmm;
            const markerDate = entry.date || today;
            timesMap[marker] = true;
            lastRunMap[entry.profileId] = { date: markerDate, times: timesMap };
            console.log("[scheduler] Posted to", profile.businessName, "from queue", markerDate, marker);
        } catch (e) {
            const last = normalizeLast(lastRunMap[entry.profileId]);
            const timesMap = last.times || {};
            const marker = entry.hhmm || hhmm;
            const markerDate = entry.date || today;
            timesMap[marker] = true;
            lastRunMap[entry.profileId] = { date: markerDate, times: timesMap };
            console.error(
                "[scheduler] Failed from queue for",
                profile.businessName,
                String(e && e.message ? e.message : e)
            );
        }
    }

    if (toProcess.length || queueModified) {
        await saveAutoSchedulerQueue(env, autoQueue);
    }

    await setJson(env, "schedulerLastRun", lastRunMap);
}
