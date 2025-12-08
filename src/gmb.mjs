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

const TEMPLATE_CYCLE = ["SERVICE", "OFFER", "TIP", "SOCIAL_PROOF"];
const CTA_LABELS = {
  CALL_NOW: "Call now",
  LEARN_MORE: "Learn more",
  BOOK: "Book",
  ORDER: "Order",
  SHOP: "Shop",
  SIGN_UP: "Sign up"
};

async function ensureKvTable(env) {
  await env.D1_DB.prepare(`
    CREATE TABLE IF NOT EXISTS kv (
      key TEXT PRIMARY KEY,
      value TEXT,
      updated_at TEXT NOT NULL
    );
  `).run();
}

function normalizeProfiles(list) {
  if (!Array.isArray(list)) return [];
  return list.map((p) => {
    const out = { ...(p || {}) };
    if (typeof out.disabled !== "boolean") out.disabled = false;
    if (!Array.isArray(out.neighbourhoods)) out.neighbourhoods = [];
    if (!Array.isArray(out.keywords)) out.keywords = [];
    if (!Array.isArray(out.photoPool)) out.photoPool = [];
    if (!out.profileId) out.profileId = out.locationId || "";
    return out;
  });
}

export function isProfileActive(profile) {
  return !!(profile && profile.profileId && profile.disabled !== true);
}

export async function getProfiles(env) {
  const raw = (await getJson(env, "profiles", [])) || [];
  return normalizeProfiles(raw);
}

export async function saveProfiles(env, list) {
  await setJson(env, "profiles", normalizeProfiles(list));
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
    body: row.body_json ? JSON.parse(row.body_json) : {},
    status: row.status || "QUEUED",
    postedAt: row.posted_at || null,
    lastUrl: row.last_url || ""
  }));
}

async function upsertScheduledRow(env, item) {
  await ensureScheduledTable(env);
  const bodyJson = JSON.stringify(item.body || {});
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
        JSON.stringify(item.body || {})
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
    body: row.body_json ? JSON.parse(row.body_json) : {},
    status: row.status || "QUEUED",
    postedAt: row.posted_at || null,
    lastError: row.last_error || ""
  }));
}

async function upsertPhotoRow(env, item) {
  await ensurePhotoTable(env);
  const bodyJson = JSON.stringify(item.body || {});
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
    body: payload.body || {},
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
      body: item.body || {},
      status: item.status || "QUEUED"
    });
  }
  return list.filter(Boolean);
}

export async function deletePhotoScheduled(env, id) {
  await deletePhotoRow(env, id);
}

export async function commitScheduledPosts(env, items) {
  const normalized = Array.isArray(items)
    ? items
        .map((it) => ({
          id: it.id || crypto.randomUUID(),
          runAt: new Date(it.runAt).toISOString(),
          createdAt: it.createdAt || new Date().toISOString(),
          profileId: it.profileId,
          body: it.body || {},
          status: it.status || "QUEUED"
        }))
        .filter((it) => it.profileId && it.runAt)
    : [];
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

  const keywords = Array.isArray(profile.keywords) ? profile.keywords.filter(Boolean) : [];
  const city = profile.city || profile.region || "";
  const primaryKw = keywords[0] || overrides.serviceType || profile.businessName || "local services";
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
  const keywordLine = keywords.length
    ? `${city ? city + " • " : ""}${keywords.slice(0, 3).join(" · ")}`
    : `${city || "Local"} • ${primaryKw}`;
  lines.push(keywordLine);
  lines.push(`Expert ${primaryKw} in ${city || "your area"}.`);
  if (template === "OFFER" && overrides.offerTitle) {
    lines.push(`Special: ${overrides.offerTitle}`);
  } else if (template === "SOCIAL_PROOF") {
    lines.push("Clients trust us for reliable, fast results.");
  } else if (template === "TIP") {
    lines.push(`Tip: regular ${primaryKw.toLowerCase()} keeps your property in shape.`);
  } else {
    lines.push(`Need help with ${primaryKw.toLowerCase()}? We're ready.`);
  }
  const quickLinks = buildQuickLinkLines(defaults);
  quickLinks.forEach((line) => lines.push(line));
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
    idx:
      nextState.idx != null
        ? nextState.idx
        : advance
        ? prev.idx + 1
        : prev.idx,
    lastUrl: lastUrl || prev.lastUrl || ""
  };
  await saveCycleState(env, state);
}

export async function getCycleStateForProfile(env, profileId = "") {
  const state = await getCycleState(env);
  if (!profileId) return state;
  const entry = state[profileId] || { idx: 0, lastUrl: "" };
  const nextTemplate = TEMPLATE_CYCLE[(entry.idx || 0) % TEMPLATE_CYCLE.length];
  return { ...entry, nextTemplate };
}

function buildQuickLinkLines(defaults = {}) {
  const quickLinks = [
    { label: "Reviews ➡", url: defaults.reviewLink },
    { label: "Service Area ➡", url: defaults.serviceAreaLink },
    { label: "Area Map ➡", url: defaults.areaMapLink }
  ];
  return quickLinks
    .map((q) => {
      const val = String(q.url || "").trim();
      return /^https?:\/\//i.test(val) ? `${q.label} ${val}` : "";
    })
    .filter(Boolean);
}

async function composeAiTemplatePost(env, profile, overrides = {}, basics = {}) {
  const tpl = await buildTemplatePost(env, profile, overrides, basics);
  const neighbourhood = pickNeighbourhood(profile);
  let aiSummary = "";
  let aiHashtags = [];
  try {
    const gen = await aiGenerateSummaryAndHashtags(env, profile, neighbourhood);
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
  if (!Array.isArray(images) || !images.length) throw new Error("No images provided");
  const runStart = startAt ? new Date(startAt) : new Date(Date.now() + 3600_000);
  if (isNaN(runStart.getTime())) throw new Error("Invalid startAt");
  const items = [];
  images.forEach((mediaUrl, idx) => {
    const runAt = new Date(runStart.getTime() + idx * cadenceDays * 86400000).toISOString();
    items.push({
      profileId,
      runAt,
      body: { ...body, mediaUrl, autoGenerateSummary },
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
  if (!Array.isArray(images) || !images.length) throw new Error("No images provided");
  const runStart = startAt ? new Date(startAt) : new Date(Date.now() + 3600_000);
  if (isNaN(runStart.getTime())) throw new Error("Invalid startAt");
  const profileList = await getProfiles(env);
  const profile = profileList.find((p) => p.profileId === profileId);
  const drafts = [];
  for (let idx = 0; idx < images.length; idx++) {
    const mediaUrl = images[idx];
    const runAt = new Date(runStart.getTime() + idx * cadenceDays * 86400000).toISOString();
    let postText = body.postText || "";
    let cta = body.cta || "";
    let linkUrl = body.linkUrl || "";
    if ((!postText || autoGenerateSummary) && profile) {
      const basics = await fetchLocationBasics(env, profile);
      const built = await composeAiTemplatePost(env, profile, body || {}, basics);
      const tagLine = (built.hashtags || []).join(" ");
      postText = (built.summary || "").trim();
      if (tagLine && postText.length + tagLine.length + 2 <= 1500) {
        postText += "\n\n" + tagLine;
      }
      cta = cta || built.ctaCode || "";
      linkUrl = linkUrl || built.site || "";
    }
    if (profile) {
      const quickLines = buildQuickLinkLines(profile.defaults);
      if (quickLines.length) {
        const parts = (postText || "").split(/\n+/);
        const alreadyIncluded = quickLines.every((ql) =>
          parts.some((l) => l.trim() === ql.trim())
        );
        if (!alreadyIncluded) {
          const prevIdx = parts.findIndex((l) => /^previous update:/i.test(l));
          if (prevIdx !== -1) {
            const before = parts.slice(0, prevIdx);
            const after = parts.slice(prevIdx);
            postText = [...before, ...quickLines, ...after].join("\n").trim();
          } else {
            const spacer = postText ? "\n\n" : "";
            postText = (postText + spacer + quickLines.join("\n")).trim();
          }
        }
      }
    }
    drafts.push({
      id: crypto.randomUUID(),
      runAt,
      profileId,
      body: { ...body, mediaUrl, postText, autoGenerateSummary, cta, linkUrl },
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

  const normalized = Array.isArray(items)
    ? items
        .map((it) => {
          if (!it) return null;
          if (typeof it === "string") {
            return { url: String(it).trim(), serviceType: "", captions: [], addedAt: new Date().toISOString() };
          }
          const url = String(it.url || "").trim();
          if (!/^https?:\/\//i.test(url)) return null;
          return {
            url,
            serviceType: String(it.serviceType || ""),
            captions: Array.isArray(it.captions) ? it.captions.slice(0, 5) : [],
            addedAt: it.addedAt || new Date().toISOString()
          };
        })
        .filter(Boolean)
    : [];

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
  const accountId = String(profile.accountId || "");
  const locationId = String(profile.locationId || "");
  if (!accountId || !locationId) return out;

  const url =
    "https://mybusinessbusinessinformation.googleapis.com/v1/accounts/" +
    accountId +
    "/locations/" +
    locationId +
    "?readMask=websiteUri,phoneNumbers,metadata";

  try {
    const resp = await callBusinessProfileAPI(env, "GET", url);
    const data = resp && resp.data ? resp.data : {};
    out.websiteUri = data.websiteUri || "";
    if (data.phoneNumbers && data.phoneNumbers.primaryPhone) {
      out.primaryPhone = String(data.phoneNumbers.primaryPhone);
    }
    const meta = data.metadata || {};
    out.mapsUri = meta.mapsUri || "";
    out.placeReviewUri = meta.placeReviewUri || "";
    return out;
  } catch (e) {
    console.error("fetchLocationBasics error:", e);
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
      typeof linkUrl === "string" && linkUrl.toLowerCase().startsWith("tel:")
        ? linkUrl.replace(/^tel:/i, "").trim()
        : "";
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
    id:
      String(Date.now()) +
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
    gmbPostId: payload.gmbPostId || ""
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

function resolveMediaUrl(env, mediaUrlRaw) {
  const url = String(mediaUrlRaw || "").trim();
  if (!url) throw new Error("Missing mediaUrl");
  if (/^https?:\/\//i.test(url)) return url;
  const base = env.PUBLIC_BASE_URL || env.PUBLIC_MEDIA_BASE || "";
  if (!base) {
    throw new Error("mediaUrl must be https://... or set PUBLIC_BASE_URL to prefix /uploads files");
  }
  const cleanedBase = base.replace(/\/+$/, "");
  const cleanedPath = url.startsWith("/") ? url : "/" + url;
  return cleanedBase + cleanedPath;
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
  const res = await callBusinessProfileApi(env, url, { method: "GET" }).catch(async (e) => {
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
    profile.accountId
      ? `accounts/${profile.accountId}/locations/${profile.locationId}/media?pageSize=${pageSize}`
      : null
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
      profile.accountId
        ? `accounts/${profile.accountId}/locations/${profile.locationId}/media?pageSize=${pageSize}${tokenPart}`
        : null
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

  const defaults = (profile && profile.defaults) || {};
  const ctaCode = (body && body.cta) || ctaFromTemplate || defaults.cta || "LEARN_MORE";
  let linkUrl =
    (body && body.linkUrl) ||
    linkOverride ||
    defaults.linkUrl ||
    basics.websiteUri ||
    profile.landingUrl ||
    "";
  const phoneOverride = (body && body.phone) || defaults.phone || "";
  const providedLinkUrl = linkUrl;
  const siteCandidate = basics.websiteUri || profile.landingUrl || "";
  let mediaUrlRaw = (body && body.mediaUrl) || defaults.mediaUrl || "";
  let usedFromPool = false;
  let usedPoolEntry = null;

  const ctaObj = buildCallToAction(ctaCode, linkUrl, basics, profile, phoneOverride);
  // Safety: CALL should never include a url to satisfy GBP validation
  const safeCta =
    ctaObj && ctaObj.actionType === "CALL"
      ? { actionType: "CALL" }
      : ctaObj;
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
    if (isHttpsImage(url)) {
      mediaUrlRaw = url;
      usedFromPool = true;
      usedPoolEntry = { url, serviceType, captions };
      // If a caption exists, prefer the first as post text when none supplied
      if (!summary && captions && captions.length) {
        summary = captions[0];
      }
      if (!postType && serviceType) {
        // no-op; could map service type to topic if needed
      }
    }
  }

  if (mediaUrlRaw && isHttpsImage(mediaUrlRaw)) {
    payload.media = [{ mediaFormat: "PHOTO", sourceUrl: mediaUrlRaw }];
    usedImageUrl = mediaUrlRaw;
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
      postedUrl
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
          const updated = Array.isArray(list[idx].photoPool)
            ? list[idx].photoPool.slice(1)
            : [];
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
        postedUrl
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
      gmbPostId: ""
    });
    throw err;
  }
}

// Scheduler helpers
export async function getSchedulerConfig(env) {
  const raw = (await getJson(env, "schedulerConfig", null)) || null;
  const cfg = { ...DEFAULT_SCHED, ...(raw || {}) };
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
  const cfg = { ...old };
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
    const lastDate = lastRunRaw && lastRunRaw.date
      ? new Date(lastRunRaw.date)
      : lastRunRaw
      ? new Date(lastRunRaw)
      : null;
    const nextDate = lastDate
      ? new Date(new Date(lastDate).setDate(lastDate.getDate() + intervalDays))
      : null;
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
  for (const p of profilesForDaily) {
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
    body: payload.body || {},
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
    body: updates.body || current.body,
    status: current.status || "QUEUED"
  };
  await upsertScheduledRow(env, updated);
  return updated;
}

export async function deleteScheduledPost(env, id) {
  await deleteScheduledRow(env, id);
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
        const body = {
          profileId: item.profileId,
          mediaUrl: item.body?.mediaUrl || "",
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
        const body = { profileId: item.profileId, ...(item.body || {}) };
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
  if (!cfg.enabled) return;

  const profilesForDaily = profiles;
  let lastRunMap = (await getJson(env, "schedulerLastRun", {})) || {};
  if (!lastRunMap || typeof lastRunMap !== "object") lastRunMap = {};

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

    try {
      await postToGmb(env, { profileId: p.profileId });
      const timesMap = last.times || {};
      timesMap[hhmm] = true;
      lastRunMap[p.profileId] = { date: today, times: timesMap };
      console.log("[scheduler] Posted to", p.businessName, "at", hhmm);
    } catch (e) {
      const timesMap = last.times || {};
      timesMap[hhmm] = true;
      lastRunMap[p.profileId] = { date: today, times: timesMap };
      console.error(
        "[scheduler] Failed for",
        p.businessName,
        String(e && e.message ? e.message : e)
      );
    }
  }

  await setJson(env, "schedulerLastRun", lastRunMap);
}
