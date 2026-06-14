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
    updateScheduledPhoto,
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
    fetchLocationBasics,
    postToGmb
} from "./gmb.mjs";
import { aiGenerateSummaryAndHashtags, pickNeighbourhood, safeJoinHashtags } from "./ai.mjs";
import { handleYoutubeRoute } from "./routes/youtube.mjs";
import { runDueYoutubeScheduledJobs } from "./services/youtubeSchedule.mjs";

const VERSION = "1.0.0";

const CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,POST,PUT,PATCH,DELETE,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, x-agent-api-key, x-epf-webhook-secret"
};

const CITY_GEO_DEFAULTS = {
    mississauga: { lat: 43.589, lng: -79.6441 },
    toronto: { lat: 43.6532, lng: -79.3832 },
    brampton: { lat: 43.7315, lng: -79.7624 },
    oakville: { lat: 43.4675, lng: -79.6877 },
    burlington: { lat: 43.3255, lng: -79.799 },
    hamilton: { lat: 43.2557, lng: -79.8711 },
    milton: { lat: 43.5183, lng: -79.8774 },
    etobicoke: { lat: 43.6205, lng: -79.5132 },
    vaughan: { lat: 43.8563, lng: -79.5085 },
    markham: { lat: 43.8561, lng: -79.337 },
    "richmond hill": { lat: 43.8828, lng: -79.4403 },
    calgary: { lat: 51.0447, lng: -114.0719 }
};

const CEILING_CUSTOM_GBP_SERVICES = [
    "Popcorn Ceiling Removal Hamilton",
    "Stucco Ceiling Removal Hamilton",
    "Stoney Creek Popcorn Ceiling Removal",
    "Painted Popcorn Ceiling Removal",
    "Smooth Ceiling Finish",
    "Level 5 Ceiling Skim Coat",
    "Ceiling Skim Coating",
    "Ceiling Drywall Repair",
    "Ceiling Painting",
    "Basement Popcorn Ceiling Removal",
    "Condo Popcorn Ceiling Removal",
    "Pot Light Ceiling Patching",
    "Dust-Controlled Ceiling Removal",
    "HEPA Sanding Ceiling Service",
    "Water Damage Ceiling Repair",
    "Textured Ceiling Removal",
    "Stipple Ceiling Removal",
    "Ceiling Primer and Paint",
    "Plaster Ceiling Repair",
    "Smooth Ceiling After Popcorn Removal"
];

const BLOG_WEBHOOK_EVENTS_KEY = "blogWebhookEvents";
const MAX_BLOG_WEBHOOK_EVENTS = 250;
const EPF_POPCORN_PROFILE_ID = "profile-116118369255335894193-5223601481889907889";
const HAMILTON_STONEY_PROFILE_ID = "profile-116118369255335894193-16548860165116757481";

const CITY_POSTAL_CODE_HINTS = {
    burlington: ["L7R", "L7N", "L7M", "L7T", "L7L"],
    hamilton: ["L8P", "L8S", "L8L", "L8M", "L8N", "L8K", "L9H"],
    "stoney creek": ["L8E", "L8G", "L8J", "L8K"],
    mississauga: ["L5A", "L5B", "L5C", "L5M", "L5N", "L5L"],
    oakville: ["L6H", "L6K", "L6L", "L6M", "L6J"],
    grimsby: ["L3M"],
    milton: ["L9T", "L9E"],
    etobicoke: ["M8V", "M8W", "M8X", "M9B", "M9C", "M9V", "M9W"]
};

const CITY_AREA_HINTS = {
    burlington: ["Aldershot", "Roseland", "Shoreacres", "Headon Forest", "Tyandaga", "Millcroft"],
    hamilton: ["Ancaster", "Waterdown", "Dundas", "Binbrook", "Hamilton Mountain", "Westdale"],
    "stoney creek": ["Fruitland", "Battlefield", "Winona", "Heritage Green", "Dewitt", "Dalegrove"],
    mississauga: ["Port Credit", "Cooksville", "Erin Mills", "Meadowvale", "Clarkson", "Lorne Park"],
    oakville: ["Bronte", "Glen Abbey", "River Oaks", "West Oak Trails", "Old Oakville", "College Park"]
};

function getServiceItemName(item) {
    return String(
        item?.freeFormServiceItem?.label?.displayName ||
        item?.structuredServiceItem?.serviceTypeId ||
        ""
    ).trim();
}

function mergeFreeFormServiceItems(existingItems = [], customServices = [], category = "", languageCode = "en") {
    const merged = Array.isArray(existingItems) ? [...existingItems] : [];
    const seen = new Set(merged.map((item) => getServiceItemName(item).toLowerCase()).filter(Boolean));
    for (const service of customServices) {
        const displayName = String(service || "").trim();
        if (!displayName) continue;
        const key = displayName.toLowerCase();
        if (seen.has(key)) continue;
        seen.add(key);
        merged.push({
            freeFormServiceItem: {
                category,
                label: {
                    displayName,
                    languageCode
                }
            }
        });
    }
    return merged;
}

function buildEpfImageBrandingPrompt(prompt = "", context = {}) {
    const base = String(prompt || "").trim();
    const service = String(context.serviceType || context.service || context.theme || "Popcorn Ceiling Removal").trim();
    const city = String(context.city || "").trim();
    const businessName = String(context.businessName || "EPF Pro Services").trim();
    const serviceKeyword = /popcorn|ceiling|stucco/i.test(service) ? "Popcorn Ceiling Removal" : service;
    const cityLine = city ?
        `Second banner line: readable city/location text, spelled exactly "${city}".` :
        "Second banner line: readable city/location text when a city is known; do not invent one.";
    const brandText = businessName.toLowerCase().includes("epf") ? businessName : "EPF Pro Services";
    const requiredOverlay = [
        "Mandatory EPF Pro Services marketing overlay:",
        `Top dark navy banner with large bold white SEO keyword text, spelled exactly "${serviceKeyword}".`,
        cityLine,
        `Small clean brand text: "${brandText}".`,
        "Bottom tagline in white, if readable: CLEAN. MODERN. TRANSFORMED.",
        "Use the same contractor ad style as a premium Google Business Profile image: protected living room, plastic sheeting, ladder, compound bucket, vacuum sander or scraper, smooth finished ceiling, warm beige/gold location strip.",
        "Text must be crisp, high contrast, centered, correctly spelled, and not warped."
    ].join(" ");
    const negative = [
        "Do not include unrelated services, pest control, insects, rodents, random fake brands, unreadable text, fake phone numbers, watermarks, distorted ladders, warped rooms, or customer faces."
    ].join(" ");
    return [base, requiredOverlay, negative].filter(Boolean).join("\n\n");
}

function normalizeBlogUrl(raw = "") {
    const text = String(raw || "").trim();
    if (!text) return "";
    try {
        const parsed = new URL(text);
        parsed.hash = "";
        parsed.searchParams.sort();
        parsed.pathname = parsed.pathname.replace(/\/+$/, "") || "/";
        return parsed.toString().toLowerCase();
    } catch (_e) {
        return text.replace(/\/+$/, "").toLowerCase();
    }
}

function slugifyPathPart(value = "") {
    return String(value || "")
        .trim()
        .toLowerCase()
        .replace(/&/g, " and ")
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/^-+|-+$/g, "");
}

function isEpfBlogUrl(raw = "") {
    try {
        const host = new URL(String(raw || "")).hostname.replace(/^www\./, "").toLowerCase();
        return host === "epfproservices.com";
    } catch (_e) {
        return String(raw || "").toLowerCase().includes("epfproservices.com");
    }
}

function firstNonEmpty(...values) {
    for (const value of values) {
        if (value === null || value === undefined) continue;
        const text = String(value).trim();
        if (text) return text;
    }
    return "";
}

function stripHtml(value = "") {
    return String(value || "")
        .replace(/<[^>]*>/g, " ")
        .replace(/\s+/g, " ")
        .trim();
}

function extractFirstUrl(value = "") {
    const match = String(value || "").match(/https?:\/\/[^\s"'<>]+/i);
    return match ? match[0].replace(/[),.;]+$/, "") : "";
}

function titleFromBlogUrl(raw = "") {
    try {
        const parsed = new URL(String(raw || ""));
        const parts = parsed.pathname.split("/").filter(Boolean);
        const slug = parts[parts.length - 1] || "";
        return slug
            .replace(/[-_]+/g, " ")
            .replace(/\s+/g, " ")
            .trim()
            .replace(/\b\w/g, (letter) => letter.toUpperCase());
    } catch (_e) {
        return "";
    }
}

function parseBlogWebhookBody(rawText = "", contentType = "") {
    const text = String(rawText || "").trim();
    if (!text) return {};
    if (contentType.includes("application/json") || /^[\[{"]/.test(text)) {
        try {
            const parsed = JSON.parse(text);
            if (typeof parsed === "string") return { url: extractFirstUrl(parsed) || parsed };
            return parsed && typeof parsed === "object" ? parsed : {};
        } catch (_e) {
            // Fall through and treat the body as plain text.
        }
    }
    const url = extractFirstUrl(text);
    return url ? { url, rawText: text } : { rawText: text };
}

function normalizeBlogWebhookBody(body = {}) {
    const post = body.post || body.data?.post || body.data || {};
    const event = firstNonEmpty(
        body.event,
        body.eventType,
        body.type,
        body.action,
        body.hook,
        post.event,
        post.eventType,
        post.type,
        post.action
    );
    const status = firstNonEmpty(
        body.status,
        body.post_status,
        body.postStatus,
        body.data?.status,
        body.data?.post_status,
        post.status,
        post.post_status,
        post.postStatus
    );
    const url = firstNonEmpty(
        body.url,
        body.link,
        body.permalink,
        body.postUrl,
        body.post_url,
        body.guid?.rendered,
        body.data?.url,
        body.data?.link,
        body.data?.permalink,
        post.url,
        post.link,
        post.permalink,
        post.postUrl,
        post.post_url,
        post.guid?.rendered
    );
    const title = stripHtml(firstNonEmpty(
        body.title?.rendered,
        body.title,
        body.post_title,
        body.postTitle,
        body.data?.title?.rendered,
        body.data?.title,
        body.data?.post_title,
        post.title?.rendered,
        post.title,
        post.post_title,
        post.postTitle,
        titleFromBlogUrl(url)
    ));
    const excerpt = stripHtml(firstNonEmpty(
        body.excerpt?.rendered,
        body.excerpt,
        body.summary,
        body.description,
        body.data?.excerpt?.rendered,
        body.data?.excerpt,
        post.excerpt?.rendered,
        post.excerpt,
        post.summary,
        post.description
    ));
    return {
        ...body,
        event,
        status,
        url,
        title,
        excerpt,
        city: firstNonEmpty(body.city, body.location, body.data?.city, post.city, post.location),
        service: firstNonEmpty(body.service, body.serviceType, body.category, body.data?.service, post.service, post.serviceType, post.category),
        publishedAt: firstNonEmpty(
            body.publishedAt,
            body.published_at,
            body.date,
            body.post_date,
            body.data?.publishedAt,
            body.data?.date,
            post.publishedAt,
            post.published_at,
            post.date,
            post.post_date
        )
    };
}

function isAcceptedBlogCreatedEvent(payload = {}) {
    const event = String(payload.event || "").trim().toLowerCase().replace(/[^a-z0-9]+/g, "_");
    const status = String(payload.status || "").trim().toLowerCase();
    const url = String(payload.url || "").trim();
    const acceptedEvents = new Set([
        "blog_post_created",
        "blog_post_published",
        "post_created",
        "post_published",
        "publish_post",
        "published_post",
        "created",
        "published"
    ]);
    if (acceptedEvents.has(event)) return true;
    if (!event && ["publish", "published"].includes(status)) return true;
    if (!event && !status && isEpfBlogUrl(url)) return true;
    if (event.includes("publish") && ["", "publish", "published"].includes(status)) return true;
    return false;
}

function getBlogCityKey(payload = {}) {
    const city = String(payload.city || "").trim().toLowerCase();
    const url = String(payload.url || "").toLowerCase();
    const title = String(payload.title || "").toLowerCase();
    const text = `${city} ${url} ${title}`;
    if (text.includes("stoney-creek") || text.includes("stoney creek")) return "stoney creek";
    if (text.includes("hamilton")) return "hamilton";
    if (text.includes("burlington")) return "burlington";
    if (text.includes("mississauga")) return "mississauga";
    if (text.includes("oakville")) return "oakville";
    if (text.includes("grimsby")) return "grimsby";
    if (text.includes("milton")) return "milton";
    if (text.includes("etobicoke")) return "etobicoke";
    return city;
}

function inferBlogServiceType(payload = {}) {
    const text = [
        payload.service,
        payload.title,
        payload.excerpt,
        payload.url
    ].map((value) => String(value || "").toLowerCase()).join(" ");

    if (/\bdrywall\b/.test(text)) {
        if (/install|installation|boarding|sheetrock|hang/.test(text)) return "Drywall Installation";
        if (/patch|patching|repair|hole|crack|water damage|ceiling repair/.test(text)) return "Drywall Repair and Patching";
        return "Drywall Repair";
    }

    if (/baseboard|trim|moulding|molding|millwork/.test(text)) {
        if (/install|installation/.test(text)) return "Baseboard Installation";
        if (/repair|replace|replacement/.test(text)) return "Baseboard Repair and Replacement";
        return "Baseboard and Trim";
    }

    if (/popcorn|stucco|stipple|textured ceiling|ceiling removal|smooth ceiling|skim coat|ceiling skim/.test(text)) {
        return "Popcorn Ceiling Removal";
    }

    return String(payload.service || payload.title || "Popcorn Ceiling Removal").trim();
}

function getBlogCityLandingUrl(payload = {}, profile = null) {
    const cityKey = getBlogCityKey(payload);
    const service = inferBlogServiceType(payload).toLowerCase();
    if (service.includes("baseboard")) {
        if (cityKey) return `https://epfproservices.com/services/baseboard-installation/${slugifyPathPart(cityKey)}/`;
        return "https://epfproservices.com/services/baseboard-installation/";
    }
    if (service.includes("popcorn") || String(payload.url || "").toLowerCase().includes("popcorn")) {
        const map = {
            "stoney creek": "https://epfproservices.com/popcorn-ceiling-removal/hamilton/stoney-creek/",
            hamilton: "https://epfproservices.com/popcorn-ceiling-removal/hamilton/",
            burlington: "https://epfproservices.com/popcorn-ceiling-removal/burlington/",
            mississauga: "https://epfproservices.com/popcorn-ceiling-removal/mississauga/",
            oakville: "https://epfproservices.com/popcorn-ceiling-removal/oakville/",
            grimsby: "https://epfproservices.com/popcorn-ceiling-removal/grimsby/",
            milton: "https://epfproservices.com/popcorn-ceiling-removal/milton/",
            etobicoke: "https://epfproservices.com/popcorn-ceiling-removal/etobicoke/"
        };
        if (map[cityKey]) return map[cityKey];
        if (cityKey) return `https://epfproservices.com/popcorn-ceiling-removal/${slugifyPathPart(cityKey)}/`;
    }
    if (profile?.defaults?.linkUrl) return profile.defaults.linkUrl;
    return profile?.landingUrl || String(payload.url || "").trim();
}

function scoreBlogProfile(profile, payload = {}) {
    if (!profile || profile.disabled) return -1;
    const city = String(payload.city || "").trim().toLowerCase();
    const service = String(payload.service || "").trim().toLowerCase();
    const url = String(payload.url || "").trim().toLowerCase();
    const fields = [
        profile.businessName,
        profile.city,
        profile.landingUrl,
        profile.defaults?.linkUrl,
        ...(Array.isArray(profile.keywords) ? profile.keywords : []),
        ...(Array.isArray(profile.serviceTopics) ?
            profile.serviceTopics.flatMap((topic) => [
                topic?.label,
                topic?.serviceType,
                topic?.primaryKeyword,
                topic?.cityKeyword,
                topic?.secondaryKeywords,
                topic?.landingUrl
            ]) : [])
    ].map((v) => String(v || "").toLowerCase());
    const haystack = fields.join(" ");
    let score = 0;
    if (city && haystack.includes(city)) score += 25;
    if (city && String(profile.city || "").toLowerCase() === city) score += 30;
    if (service && haystack.includes(service)) score += 20;
    if (service.includes("popcorn") && haystack.includes("popcorn")) score += 15;
    try {
        const host = profile.landingUrl ? new URL(profile.landingUrl).hostname.replace(/^www\./, "") : "";
        if (url && host && url.includes(host)) score += 5;
    } catch (_e) {}
    if (String(profile.businessName || "").toLowerCase().includes("popcorn")) score += 5;
    return score;
}

function findBlogTargetProfile(profiles = [], payload = {}) {
    const profileId = String(payload.profileId || "").trim();
    if (profileId) {
        return profiles.find((profile) => profile && profile.profileId === profileId) || null;
    }
    const cityKey = getBlogCityKey(payload);
    if (isEpfBlogUrl(payload.url)) {
        const forcedId =
            cityKey === "hamilton" || cityKey === "stoney creek" ?
            HAMILTON_STONEY_PROFILE_ID :
            EPF_POPCORN_PROFILE_ID;
        const forced = profiles.find((profile) => profile && profile.profileId === forcedId && !profile.disabled);
        if (forced) return forced;
    }
    return profiles
        .map((profile) => ({ profile, score: scoreBlogProfile(profile, payload) }))
        .filter((entry) => entry.score > 0)
        .sort((a, b) => b.score - a.score)[0]?.profile || null;
}

function fallbackBlogDraft(payload = {}, profile = null) {
    const title = String(payload.title || "").trim();
    const excerpt = String(payload.excerpt || "").trim();
    const url = String(payload.url || "").trim();
    const city = String(payload.city || profile?.city || "").trim();
    const service = String(payload.service || "home renovation").trim();
    const tagValue = (value) => String(value || "")
        .replace(/[^a-zA-Z0-9]+/g, " ")
        .trim()
        .split(/\s+/)
        .filter(Boolean)
        .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
        .join("");
    const tags = safeJoinHashtags([
        tagValue(service),
        tagValue(city),
        "EPFProServices",
        "GoogleBusinessProfile"
    ], 180);
    const text = [
        title,
        excerpt,
        city ? `Serving ${city} with ${service}.` : `Service: ${service}.`,
        url ? `Read more: ${url}` : "",
        tags
    ].filter(Boolean).join("\n\n");
    return {
        postText: text.slice(0, 1500),
        cta: "LEARN_MORE",
        linkUrl: url
    };
}

function appendBlogContextToPost(postText = "", blogUrl = "", localSeo = null) {
    const extraLines = [localSeo?.text || ""].filter(Boolean).join("\n");
    if (!extraLines) return String(postText || "").slice(0, 1500);
    const base = String(postText || "").trim();
    if (base.length + extraLines.length + 2 <= 1500) {
        return `${base}\n\n${extraLines}`.trim();
    }
    return base.slice(0, 1500);
}

function cleanBlogAutomationPostText(postText = "") {
    const lines = String(postText || "").split("\n");
    return lines
        .filter((line) => {
            const trimmed = line.trim();
            if (/^Previous update:/i.test(trimmed)) return false;
            if (/^More info:\s*https?:\/\//i.test(trimmed)) return false;
            if (/^Blog article:\s*https?:\/\//i.test(trimmed)) return false;
            if (/^(Reviews|Service Area|Area Map)\s*►\s*https?:\/\//i.test(trimmed)) return false;
            return true;
        })
        .join("\n")
        .replace(/\n{3,}/g, "\n\n")
        .trim()
        .slice(0, 1500);
}

function seededPick(list = [], seed = "", count = 3) {
    const source = Array.isArray(list) ? list.filter(Boolean) : [];
    if (!source.length) return [];
    let hash = 0;
    const seedText = String(seed || Date.now());
    for (let i = 0; i < seedText.length; i++) {
        hash = ((hash << 5) - hash + seedText.charCodeAt(i)) | 0;
    }
    const picked = [];
    const pool = source.slice();
    while (pool.length && picked.length < count) {
        const idx = Math.abs(hash + picked.length * 17) % pool.length;
        picked.push(pool.splice(idx, 1)[0]);
    }
    return picked;
}

async function getBlogLocalSeoContext(env, profile, payload = {}) {
    const cityKey = getBlogCityKey(payload);
    const city = String(payload.city || cityKey || profile?.city || "").trim();
    const seed = `${payload.url || ""}|${payload.title || ""}|${new Date().toISOString().slice(0, 10)}`;
    let street = "";
    let primaryPostal = "";
    let mapAreas = [];
    if (profile?.locationId) {
        try {
            const locationName = `locations/${profile.locationId}`;
            const getUrl =
                `https://mybusinessbusinessinformation.googleapis.com/v1/${locationName}` +
                `?readMask=${encodeURIComponent("storefrontAddress,serviceArea")}`;
            const location = await callBusinessProfileApi(env, getUrl);
            const address = location?.storefrontAddress || {};
            street = Array.isArray(address.addressLines) ? String(address.addressLines[0] || "").trim() : "";
            primaryPostal = String(address.postalCode || "").trim().split(/\s+/)[0] || "";
            mapAreas = Array.isArray(location?.serviceArea?.places?.placeInfos) ?
                location.serviceArea.places.placeInfos.map((place) => String(place.placeName || "").replace(/,\s*ON.*$/i, "").trim()) :
                [];
        } catch (e) {
            console.warn("Blog local SEO context lookup failed:", e?.message || String(e));
        }
    }
    const postalHints = [
        primaryPostal,
        ...(CITY_POSTAL_CODE_HINTS[cityKey] || [])
    ].filter(Boolean);
    const areaHints = [
        ...(CITY_AREA_HINTS[cityKey] || []),
        ...mapAreas
    ].filter(Boolean);
    const postalCodes = seededPick([...new Set(postalHints)], seed, 3);
    const areas = seededPick([...new Set(areaHints)], seed, 3);
    const streetPart = street ? ` near ${street}` : "";
    const areaPart = areas.length ? `, including ${areas.join(", ")}` : "";
    const postalPart = postalCodes.length ? ` Postal areas: ${postalCodes.join(", ")}.` : "";
    const sentence = city ?
        `Local SEO note: ${city}${streetPart}${areaPart}.${postalPart}` :
        `${areaPart ? `Local SEO note: ${areas.join(", ")}.` : ""}${postalPart}`;
    return {
        city,
        street,
        areas,
        postalCodes,
        text: sentence.trim()
    };
}

async function buildBlogGbpDraft(env, profile, payload = {}) {
    const url = String(payload.url || "").trim();
    const service = inferBlogServiceType(payload);
    const title = String(payload.title || "").trim();
    const excerpt = String(payload.excerpt || "").trim();
    const localSeo = await getBlogLocalSeoContext(env, profile, payload);
    const cityLandingUrl = getBlogCityLandingUrl(payload, profile);
    if (!profile) {
        const fallback = fallbackBlogDraft({ ...payload, url: cityLandingUrl || url }, null);
        return {
            ...fallback,
            postText: cleanBlogAutomationPostText(appendBlogContextToPost(fallback.postText, url, localSeo)),
            blogUrl: url,
            localSeo,
            generationMode: "fallback_no_profile"
        };
    }

    const profileForGeneration = {
        ...profile,
        city: String(payload.city || profile.city || "").trim()
    };
    const overrides = {
        serviceType: service || title,
        serviceSummary: excerpt || title,
        serviceNotes: title,
        linkUrl: cityLandingUrl || url,
        cta: "LEARN_MORE"
    };
    try {
        const basics = await fetchLocationBasics(env, profile);
        const built = await composeAiTemplatePost(env, profileForGeneration, overrides, basics);
        let postText = cleanBlogAutomationPostText((built.summary || "").trim());
        const hashtags = safeJoinHashtags(built.hashtags || [], 220);
        if (hashtags && postText.length + hashtags.length + 2 <= 1500) {
            postText += "\n\n" + hashtags;
        }
        return {
            postText: cleanBlogAutomationPostText(appendBlogContextToPost(postText || fallbackBlogDraft(payload, profile).postText, url, localSeo)),
            cta: built.ctaCode || "LEARN_MORE",
            linkUrl: cityLandingUrl || built.site || url,
            blogUrl: url,
            localSeo,
            template: built.template || "",
            neighbourhood: built.neighbourhood || "",
            generationMode: "ai"
        };
    } catch (e) {
        console.error("Blog webhook draft generation failed:", e);
        const fallback = fallbackBlogDraft({ ...payload, url: cityLandingUrl || url }, profile);
        return {
            ...fallback,
            postText: cleanBlogAutomationPostText(appendBlogContextToPost(fallback.postText, url, localSeo)),
            blogUrl: url,
            localSeo,
            generationMode: "fallback_ai_error",
            generationError: e?.message || String(e)
        };
    }
}

async function handleBlogWebhookEvents(request, env) {
    if (request.method !== "GET") {
        return jsonResponse({ success: false, error: "Method not allowed" }, 405);
    }
    const expectedSecret = String(env.EPF_WEBHOOK_SECRET || "").trim();
    const providedSecret = String(request.headers.get("x-epf-webhook-secret") || "").trim();
    if (!expectedSecret || !providedSecret || providedSecret !== expectedSecret) {
        return jsonResponse({ success: false, error: "Unauthorized webhook" }, 401);
    }
    const events = await getJson(env, BLOG_WEBHOOK_EVENTS_KEY, []);
    return jsonResponse({
        success: true,
        count: Array.isArray(events) ? events.length : 0,
        events: Array.isArray(events) ? events : []
    });
}

async function saveBlogWebhookRejection(env, payload = {}, error = "") {
    const events = await getJson(env, BLOG_WEBHOOK_EVENTS_KEY, []);
    const list = Array.isArray(events) ? events : [];
    const url = String(payload.url || "").trim();
    const normalizedUrl = url ? normalizeBlogUrl(url) : "";
    const now = new Date().toISOString();
    const record = {
        id: crypto.randomUUID(),
        status: "REJECTED",
        receivedAt: now,
        updatedAt: now,
        normalizedUrl,
        event: String(payload.event || "").trim(),
        url,
        title: String(payload.title || "").trim(),
        excerpt: String(payload.excerpt || "").trim(),
        city: String(payload.city || "").trim(),
        service: String(payload.service || "").trim(),
        publishedAt: String(payload.publishedAt || "").trim(),
        error
    };
    const withoutSameUrl = normalizedUrl ?
        list.filter((item) => item?.normalizedUrl !== normalizedUrl || item?.status === "POSTED") :
        list;
    await setJson(env, BLOG_WEBHOOK_EVENTS_KEY, [record, ...withoutSameUrl].slice(0, MAX_BLOG_WEBHOOK_EVENTS));
}

async function handleBlogAutomationStatus(request, env) {
    if (request.method !== "GET") {
        return jsonResponse({ success: false, error: "Method not allowed" }, 405);
    }
    const events = await getJson(env, BLOG_WEBHOOK_EVENTS_KEY, []);
    const list = Array.isArray(events) ? events : [];
    const posted = list.filter((item) => item?.status === "POSTED").length;
    const failed = list.filter((item) => item?.status === "POST_FAILED" || item?.status === "REJECTED").length;
    const profiles = await getProfiles(env).catch(() => []);
    const epfProfile = profiles.find((profile) => profile?.profileId === EPF_POPCORN_PROFILE_ID && !profile.disabled);
    const hamiltonProfile = profiles.find((profile) => profile?.profileId === HAMILTON_STONEY_PROFILE_ID && !profile.disabled);
    const lastFailed = list.find((item) => item?.status === "POST_FAILED");
    const minuteBuckets = new Map();
    list.forEach((item) => {
        const minute = String(item?.receivedAt || "").slice(0, 16);
        if (!minute) return;
        if (!minuteBuckets.has(minute)) minuteBuckets.set(minute, []);
        minuteBuckets.get(minute).push(item);
    });
    const batches = [...minuteBuckets.entries()]
        .map(([minute, items]) => ({
            minute,
            count: items.length,
            statuses: items.reduce((acc, item) => {
                const status = item?.status || "UNKNOWN";
                acc[status] = (acc[status] || 0) + 1;
                return acc;
            }, {}),
            items: items.slice(0, 5).map((item) => ({
                id: item.id,
                title: item.title,
                status: item.status,
                businessName: item.businessName,
                error: item.error || ""
            }))
        }))
        .sort((a, b) => b.minute.localeCompare(a.minute));
    const busyBatch = batches.find((batch) => batch.count >= 5) || null;
    const last = list[0] || null;
    const mkAgent = ({ ok, label, detail, tooltip, reason = "" }) => ({
        ok,
        label,
        detail,
        tooltip,
        reason: ok ? "Self-test passed." : reason || detail,
        checkedAt: new Date().toISOString()
    });
    return jsonResponse({
        success: true,
        agents: {
            validation: mkAgent({
                ok: !!env.EPF_WEBHOOK_SECRET,
                label: "Validation agent",
                detail: "Secret, EPF domain, event type, URL, title, and duplicate checks active",
                tooltip: "Receives the website webhook, checks x-epf-webhook-secret, validates BLOG_POST_CREATED, confirms the blog URL is from epfproservices.com, confirms URL/title, and blocks duplicate blog URLs.",
                reason: "EPF_WEBHOOK_SECRET is missing from Worker secrets."
            }),
            profileRouter: mkAgent({
                ok: !!epfProfile && !!hamiltonProfile,
                label: "Profile router",
                detail: "EPF blogs route to EPF profile; Hamilton/Stoney Creek routes to Stoney Creek",
                tooltip: "Chooses the EPF Pro Services popcorn profile for normal EPF blog posts and the Stoney Creek/Hamilton profile for Hamilton or Stoney Creek blog posts.",
                reason: !epfProfile ? "EPF popcorn profile is missing or paused." : "Hamilton/Stoney Creek profile is missing or paused."
            }),
            draftGenerator: mkAgent({
                ok: !!env.OPENAI_API_KEY,
                label: "Draft generator",
                detail: "AI post copy, generated CTA/link context, and local SEO signals active",
                tooltip: "The webhook supplies the original EPF blog URL. The poster agent derives the city service page for Learn More, includes the original blog URL in the post body, and adds local area/postal-code signals.",
                reason: "OPENAI_API_KEY is missing, so draft generation would fall back to simple copy."
            }),
            posting: mkAgent({
                ok: failed === 0,
                label: "Posting agent",
                detail: failed ? `${failed} failed event(s) need review` : "Auto-posting to GBP is active",
                tooltip: "Publishes the generated post to Google Business Profile immediately. If Google rejects the post, the event is saved as POST_FAILED with the error reason.",
                reason: lastFailed ? (lastFailed.error || "Last failed event did not include an error message.") : ""
            }),
            monitoring: mkAgent({
                ok: true,
                label: "Monitoring agent",
                detail: `${list.length} event(s) stored, ${posted} posted`,
                tooltip: "Stores webhook events, post results, duplicate protection state, and recent processing logs in D1-backed KV.",
                reason: ""
            }),
            batchControl: mkAgent({
                ok: !busyBatch,
                label: "Batch control agent",
                detail: busyBatch ? `${busyBatch.count} events arrived at ${busyBatch.minute}` : "No 5-at-once event bursts detected",
                tooltip: "Watches for bursts of five or more blog webhooks in the same minute. The panel groups those logs so the interface stays readable.",
                reason: busyBatch ? "Several webhooks arrived together. They are grouped in the batch log section; failed items show their reason." : ""
            })
        },
        totals: {
            events: list.length,
            posted,
            failed,
            batches: batches.length,
            busyBatches: batches.filter((batch) => batch.count >= 5).length
        },
        lastEvent: last ? {
            id: last.id,
            status: last.status,
            title: last.title,
            city: last.city,
            service: last.service,
            profileId: last.profileId,
            businessName: last.businessName,
            receivedAt: last.receivedAt,
            postedAt: last.postedAt || "",
            url: last.url,
            webhookBlogUrl: last.url,
            ctaUrl: last.draft?.linkUrl || "",
            blogUrl: last.draft?.blogUrl || last.url || "",
            generatedLinksNote: "Only the EPF blog URL is accepted from the webhook. CTA city page, Google/local/maps links, and local SEO lines are generated by the poster system.",
            postedUrl: last.postResult?.postedUrl || last.postResult?.data?.searchUrl || "",
            localSeo: last.draft?.localSeo || null,
            error: last.error || ""
        } : null,
        recent: list.slice(0, 8).map((item) => ({
            id: item.id,
            status: item.status,
            title: item.title,
            city: item.city,
            service: item.service,
            businessName: item.businessName,
            receivedAt: item.receivedAt,
            postedAt: item.postedAt || "",
            postedUrl: item.postResult?.postedUrl || item.postResult?.data?.searchUrl || "",
            error: item.error || ""
        })),
        batches: batches.slice(0, 8)
    });
}

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

async function handleBlogCreatedWebhook(request, env) {
    if (request.method !== "POST") {
        return jsonResponse({ success: false, error: "Method not allowed" }, 405);
    }

    const expectedSecret = String(env.EPF_WEBHOOK_SECRET || "").trim();
    const providedSecret = String(request.headers.get("x-epf-webhook-secret") || "").trim();
    if (!expectedSecret || !providedSecret || providedSecret !== expectedSecret) {
        return jsonResponse({ success: false, error: "Unauthorized webhook" }, 401);
    }

    let rawText = "";
    try {
        rawText = await request.text();
    } catch (_e) {
        return jsonResponse({ success: false, error: "Unreadable webhook body" }, 400);
    }

    const body = parseBlogWebhookBody(rawText, String(request.headers.get("content-type") || "").toLowerCase());
    const payload = normalizeBlogWebhookBody(body || {});
    const {
        event,
        url,
        title,
        excerpt,
        city,
        service,
        publishedAt
    } = payload;

    const existingEvents = await getJson(env, BLOG_WEBHOOK_EVENTS_KEY, []);
    const events = Array.isArray(existingEvents) ? existingEvents : [];

    if (!isAcceptedBlogCreatedEvent(payload)) {
        await saveBlogWebhookRejection(env, payload, "Invalid event type");
        return jsonResponse({ success: false, error: "Invalid event type" }, 400);
    }

    if (!url || !title) {
        await saveBlogWebhookRejection(env, payload, "Missing blog url or title");
        return jsonResponse({ success: false, error: "Missing blog url or title" }, 400);
    }

    if (!isEpfBlogUrl(url)) {
        await saveBlogWebhookRejection(env, payload, "Only epfproservices.com blog URLs are accepted for this automation");
        return jsonResponse({
            success: false,
            error: "Only epfproservices.com blog URLs are accepted for this automation"
        }, 400);
    }

    const normalizedUrl = normalizeBlogUrl(url);
    const duplicate = events.find((item) => item && item.normalizedUrl === normalizedUrl);
    if (duplicate && duplicate.status !== "POST_FAILED") {
        return jsonResponse({
            success: false,
            error: "Duplicate blog url",
            duplicate: {
                id: duplicate.id,
                receivedAt: duplicate.receivedAt,
                status: duplicate.status || ""
            }
        }, 409);
    }

    const profiles = await getProfiles(env);
    const targetProfile = findBlogTargetProfile(profiles, payload);
    if (!targetProfile) {
        await saveBlogWebhookRejection(env, payload, "No matching GBP profile found for blog city/service");
        return jsonResponse({ success: false, error: "No matching GBP profile found for blog city/service" }, 400);
    }
    const imageServiceType = inferBlogServiceType(payload);
    const cityKey = getBlogCityKey(payload);
    const draft = await buildBlogGbpDraft(env, targetProfile, payload);
    const now = new Date().toISOString();
    const record = {
        id: crypto.randomUUID(),
        status: "POSTING",
        receivedAt: now,
        updatedAt: now,
        normalizedUrl,
        event,
        url,
        title,
        excerpt: excerpt || "",
        city: city || "",
        service: service || "",
        imageServiceType,
        publishedAt: publishedAt || "",
        profileId: targetProfile?.profileId || "",
        businessName: targetProfile?.businessName || "",
        draft
    };

    let postResult = null;
    try {
        postResult = await postToGmb(env, {
            profileId: targetProfile.profileId,
            postText: draft.postText,
            cta: draft.cta || "LEARN_MORE",
            linkUrl: draft.linkUrl || url,
            serviceType: imageServiceType,
            theme: imageServiceType,
            city: city || cityKey || targetProfile.city || "",
            businessName: targetProfile.businessName || "EPF Pro Services",
            topicType: "STANDARD",
            mediaPending: true,
            forceGenerateMedia: true
        });
        record.status = "POSTED";
        record.postedAt = new Date().toISOString();
        record.updatedAt = record.postedAt;
        record.postResult = postResult;
    } catch (e) {
        record.status = "POST_FAILED";
        record.updatedAt = new Date().toISOString();
        record.error = e?.message || String(e);
        await setJson(env, BLOG_WEBHOOK_EVENTS_KEY, [record, ...events].slice(0, MAX_BLOG_WEBHOOK_EVENTS));
        console.error("Blog webhook auto-post failed:", record);
        return jsonResponse({
            success: false,
            error: "GBP auto-post failed",
            data: {
                id: record.id,
                url,
                title,
                city,
                service,
                status: record.status,
                profileId: record.profileId,
                businessName: record.businessName,
                draft,
                postError: record.error
            }
        }, 500);
    }

    await setJson(env, BLOG_WEBHOOK_EVENTS_KEY, [record, ...events.filter((item) => item?.normalizedUrl !== normalizedUrl)].slice(0, MAX_BLOG_WEBHOOK_EVENTS));

    console.log("New blog webhook received:", {
        id: record.id,
        url,
        title,
        excerpt,
        city,
        service,
        imageServiceType,
        publishedAt,
        profileId: record.profileId,
        status: record.status
    });

    return jsonResponse({
        success: true,
        message: "Blog webhook received and GBP post published",
        data: {
            id: record.id,
            url,
            title,
            city,
            service,
            imageServiceType,
            status: record.status,
            profileId: record.profileId,
            businessName: record.businessName,
            draft,
            postResult
        }
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
    const geoProperties = {
        city: { type: "string", description: "City to use in the prompt and for fallback geotagging when known." },
        cities: { type: "array", items: { type: "string" }, description: "Optional per-image city list." },
        neighbourhood: { type: "string" },
        neighbourhoods: { type: "array", items: { type: "string" }, description: "Optional per-image neighbourhood list." },
        lat: { type: "number", description: "Latitude to embed into JPEG EXIF GPS metadata." },
        lng: { type: "number", description: "Longitude to embed into JPEG EXIF GPS metadata." },
        latitude: { type: "number" },
        longitude: { type: "number" },
        photoLat: { type: "number" },
        photoLng: { type: "number" },
        geo: {
            type: "object",
            description: "Single-image geotag alias. Use geoLocations for batches.",
            properties: {
                city: { type: "string" },
                neighbourhood: { type: "string" },
                lat: { type: "number" },
                lng: { type: "number" },
                latitude: { type: "number" },
                longitude: { type: "number" }
            }
        },
        geotag: {
            type: "object",
            description: "Single-image geotag alias.",
            properties: {
                city: { type: "string" },
                neighbourhood: { type: "string" },
                lat: { type: "number" },
                lng: { type: "number" },
                latitude: { type: "number" },
                longitude: { type: "number" }
            }
        },
        gpsExif: {
            type: "object",
            description: "Single-image EXIF GPS alias.",
            properties: {
                lat: { type: "number" },
                lng: { type: "number" },
                latitude: { type: "number" },
                longitude: { type: "number" }
            }
        },
        geoText: {
            type: "string",
            description: "Single-image geotag as text. Format: city|lat|lng or city,lat,lng. Use this if numeric coordinate fields are rejected by the agent runtime."
        },
        geoTexts: {
            type: "array",
            description: "Per-image geotags as text. Each item format: city|lat|lng or city,lat,lng.",
            items: { type: "string" }
        },
        geoLocations: {
            type: "array",
            description: "Per-image geotags. Each generated/uploaded JPEG is stamped with matching EXIF GPS coordinates.",
            items: {
                type: "object",
                properties: {
                    city: { type: "string" },
                    neighbourhood: { type: "string" },
                    lat: { type: "number" },
                    lng: { type: "number" },
                    latitude: { type: "number" },
                    longitude: { type: "number" }
                }
            }
        }
    };
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
                                    serviceType: { type: "string" },
                                    ...geoProperties
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
                                    addToProfile: { type: "boolean", default: true },
                                    ...geoProperties
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
                                    linkUrl: { type: "string" },
                                    ...geoProperties
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
                                    autoGenerateSummary: { type: "boolean", default: true },
                                    background: {
                                        type: "boolean",
                                        default: true,
                                        description: "Return quickly and finish image generation/scheduling in the background. Recommended for multi-image schedules to avoid agent action timeouts."
                                    },
                                    ...geoProperties
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
                                    quality: { type: "string", default: "high" },
                                    ...geoProperties
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

function concatUint8Arrays(parts = []) {
    const total = parts.reduce((sum, part) => sum + part.length, 0);
    const out = new Uint8Array(total);
    let offset = 0;
    for (const part of parts) {
        out.set(part, offset);
        offset += part.length;
    }
    return out;
}

function textBytes(text = "") {
    return new TextEncoder().encode(String(text || ""));
}

function rationalBytes(value, denominator = 1000000) {
    const v = Math.max(0, Math.round(Math.abs(value) * denominator));
    return [v >>> 24, (v >>> 16) & 255, (v >>> 8) & 255, v & 255, denominator >>> 24, (denominator >>> 16) & 255, (denominator >>> 8) & 255, denominator & 255];
}

function dmsRationals(value) {
    const abs = Math.abs(value);
    const deg = Math.floor(abs);
    const minFloat = (abs - deg) * 60;
    const min = Math.floor(minFloat);
    const sec = (minFloat - min) * 60;
    return [
        ...rationalBytes(deg, 1),
        ...rationalBytes(min, 1),
        ...rationalBytes(sec, 1000000)
    ];
}

function tiffAsciiEntry(tag, value = "", valueOffset = 0) {
    const bytes = textBytes(`${value}\0`);
    const count = bytes.length;
    const valueField = count <= 4 ?
        [...bytes, 0, 0, 0, 0].slice(0, 4) :
        [valueOffset >>> 24, (valueOffset >>> 16) & 255, (valueOffset >>> 8) & 255, valueOffset & 255];
    return [
        (tag >>> 8) & 255, tag & 255,
        0, 2,
        count >>> 24, (count >>> 16) & 255, (count >>> 8) & 255, count & 255,
        ...valueField
    ];
}

function tiffLongEntry(tag, value = 0) {
    return [
        (tag >>> 8) & 255, tag & 255,
        0, 4,
        0, 0, 0, 1,
        value >>> 24, (value >>> 16) & 255, (value >>> 8) & 255, value & 255
    ];
}

function tiffRationalEntry(tag, count, valueOffset) {
    return [
        (tag >>> 8) & 255, tag & 255,
        0, 5,
        count >>> 24, (count >>> 16) & 255, (count >>> 8) & 255, count & 255,
        valueOffset >>> 24, (valueOffset >>> 16) & 255, (valueOffset >>> 8) & 255, valueOffset & 255
    ];
}

function jpegHasExif(bytes) {
    let offset = 2;
    while (offset + 4 < bytes.length && bytes[offset] === 255) {
        const marker = bytes[offset + 1];
        const len = (bytes[offset + 2] << 8) | bytes[offset + 3];
        if (marker === 225 && offset + 10 < bytes.length) {
            return bytes[offset + 4] === 69 && bytes[offset + 5] === 120 && bytes[offset + 6] === 105 && bytes[offset + 7] === 102;
        }
        if (marker === 218 || marker === 217 || len < 2) break;
        offset += 2 + len;
    }
    return false;
}

function buildGpsExifSegment(meta = {}) {
    const lat = parseFloat(meta.lat);
    const lng = parseFloat(meta.lng);
    if (Number.isNaN(lat) || Number.isNaN(lng)) return null;
    const desc = [
            meta.businessName,
            meta.city,
            meta.neighbourhood,
            meta.serviceType || meta.serviceKeywords,
            meta.categoryKeywords,
            meta.website
        ]
        .map((s) => String(s || "").trim())
        .filter(Boolean)
        .join(" | ");

    const descBytes = desc ? textBytes(`${desc}\0`) : new Uint8Array();
    const ifd0Count = desc ? 3 : 2;
    const ifd0Start = 8;
    const ifd0Size = 2 + ifd0Count * 12 + 4;
    const gpsIfdOffset = ifd0Start + ifd0Size + descBytes.length;
    const gpsCount = 4;
    const gpsIfdSize = 2 + gpsCount * 12 + 4;
    const gpsDataOffset = gpsIfdOffset + gpsIfdSize;
    const latOffset = gpsDataOffset;
    const lngOffset = latOffset + 24;

    const ifd0Entries = [];
    if (desc) {
        ifd0Entries.push(tiffAsciiEntry(0x010e, desc, ifd0Start + ifd0Size));
    }
    ifd0Entries.push(tiffAsciiEntry(0x013b, "AI"));
    ifd0Entries.push(tiffLongEntry(0x8825, gpsIfdOffset));

    const gpsEntries = [
        tiffAsciiEntry(0x0001, lat >= 0 ? "N" : "S"),
        tiffRationalEntry(0x0002, 3, latOffset),
        tiffAsciiEntry(0x0003, lng >= 0 ? "E" : "W"),
        tiffRationalEntry(0x0004, 3, lngOffset)
    ];

    const tiff = concatUint8Arrays([
        new Uint8Array([0x4d, 0x4d, 0x00, 0x2a, 0x00, 0x00, 0x00, 0x08]),
        new Uint8Array([(ifd0Entries.length >>> 8) & 255, ifd0Entries.length & 255]),
        ...ifd0Entries.map((entry) => new Uint8Array(entry)),
        new Uint8Array([0, 0, 0, 0]),
        descBytes,
        new Uint8Array([(gpsEntries.length >>> 8) & 255, gpsEntries.length & 255]),
        ...gpsEntries.map((entry) => new Uint8Array(entry)),
        new Uint8Array([0, 0, 0, 0]),
        new Uint8Array(dmsRationals(lat)),
        new Uint8Array(dmsRationals(lng))
    ]);
    const payload = concatUint8Arrays([textBytes("Exif\0\0"), tiff]);
    const len = payload.length + 2;
    if (len > 65535) return null;
    return concatUint8Arrays([
        new Uint8Array([0xff, 0xe1, (len >>> 8) & 255, len & 255]),
        payload
    ]);
}

function embedGpsExifInJpeg(arrayBuffer, meta = {}) {
    const bytes = new Uint8Array(arrayBuffer);
    if (bytes.length < 4 || bytes[0] !== 0xff || bytes[1] !== 0xd8) {
        return { arrayBuffer, stamped: false, reason: "not_jpeg" };
    }
    if (jpegHasExif(bytes)) {
        return { arrayBuffer, stamped: false, reason: "existing_exif" };
    }
    const segment = buildGpsExifSegment(meta);
    if (!segment) {
        return { arrayBuffer, stamped: false, reason: "missing_geo" };
    }
    const out = concatUint8Arrays([bytes.slice(0, 2), segment, bytes.slice(2)]);
    return { arrayBuffer: out.buffer, stamped: true };
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
    const putOptions = { httpMetadata: { contentType } };
    if (options.customMetadata && typeof options.customMetadata === "object") {
        putOptions.customMetadata = options.customMetadata;
    }
    await env.MEDIA_BUCKET.put(key, arrayBuffer, putOptions);
    return { key, url: originMediaUrl(request, key) };
}

function cityGeoDefault(city = "") {
    const key = String(city || "").trim().toLowerCase();
    return CITY_GEO_DEFAULTS[key] || null;
}

function bodyValueAt(body = {}, names = [], index = 0) {
    for (const name of names) {
        const val = body[name];
        if (Array.isArray(val)) {
            if (val[index] != null) return val[index];
            if (val.length) return val[index % val.length];
        } else if (val != null && val !== "") {
            return val;
        }
    }
    return "";
}

function parseGeoText(value = "") {
    const raw = String(value || "").trim();
    if (!raw) return {};
    const parts = raw.split(/[|,]/).map((part) => part.trim()).filter(Boolean);
    if (parts.length < 3) return {};
    const latIndex = parts.findIndex((part) => /^-?\d+(\.\d+)?$/.test(part));
    if (latIndex === -1 || latIndex + 1 >= parts.length) return {};
    return {
        city: parts.slice(0, latIndex).join(", "),
        lat: parts[latIndex],
        lng: parts[latIndex + 1],
        neighbourhood: parts.slice(latIndex + 2).join(", ")
    };
}

function resolveAgentGeo(profile, body = {}, index = 0) {
    const geoList = Array.isArray(body.geoLocations) ? body.geoLocations :
        Array.isArray(body.geotags) ? body.geotags :
        Array.isArray(body.coordinates) ? body.coordinates : [];
    const textGeo = parseGeoText(bodyValueAt(body, ["geoTexts", "geoText", "geoLocationText"], index));
    const geoEntry = geoList.length ?
        (geoList[index] || geoList[index % geoList.length] || {}) :
        (
            body.geo && typeof body.geo === "object" ? body.geo :
            body.geotag && typeof body.geotag === "object" ? body.geotag :
            body.gpsExif && typeof body.gpsExif === "object" ? body.gpsExif :
            body.coordinates && typeof body.coordinates === "object" && !Array.isArray(body.coordinates) ? body.coordinates :
            textGeo
        );
    const city = String(
        geoEntry.city ||
        bodyValueAt(body, ["cities", "city"], index) ||
        profile.city ||
        ""
    ).trim();
    const neighbourhood = String(
        geoEntry.neighbourhood ||
        bodyValueAt(body, ["neighbourhoods", "neighborhoods", "neighbourhood", "neighborhood"], index) ||
        ""
    ).trim();
    const rawLat =
        geoEntry.lat ?? geoEntry.latitude ??
        bodyValueAt(body, ["latitudes", "latitude", "lat", "photoLat"], index);
    const rawLng =
        geoEntry.lng ?? geoEntry.lon ?? geoEntry.longitude ??
        bodyValueAt(body, ["longitudes", "longitude", "lng", "lon", "photoLng"], index);
    let lat = parseFloat(rawLat);
    let lng = parseFloat(rawLng);
    let source = "provided";
    if (Number.isNaN(lat) || Number.isNaN(lng)) {
        const fallback = cityGeoDefault(city);
        if (fallback) {
            lat = fallback.lat;
            lng = fallback.lng;
            source = "city_default";
        }
    }
    if (Number.isNaN(lat) || Number.isNaN(lng)) {
        return {
            city,
            neighbourhood,
            lat: "",
            lng: "",
            hasGeo: false,
            source: "none"
        };
    }
    return {
        city,
        neighbourhood,
        lat: +lat.toFixed(6),
        lng: +lng.toFixed(6),
        hasGeo: true,
        source
    };
}

function buildAgentExifMeta(profile, body = {}, index = 0) {
    const geo = resolveAgentGeo(profile || {}, body, index);
    return {
        ...geo,
        businessName: profile?.businessName || body.businessName || "",
        serviceType: body.serviceType || body.theme || body.topic || "",
        serviceKeywords: body.serviceKeywords || body.keywords || body.serviceType || "",
        categoryKeywords: body.categoryKeywords || body.categories || "",
        website: body.website || body.websiteUrl || profile?.landingUrl || profile?.websiteUri || ""
    };
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
        const exifMeta = body.exifMeta || body.geoMeta || null;
        let bytesToSave = arrayBuf;
        let exif = { stamped: false, reason: "not_requested" };
        if (exifMeta && exifMeta.hasGeo !== false) {
            try {
                exif = embedGpsExifInJpeg(arrayBuf, exifMeta);
                bytesToSave = exif.arrayBuffer;
            } catch (err) {
                console.warn("EXIF GPS stamping failed", err?.message || err);
                exif = { stamped: false, reason: `stamp_failed: ${String(err?.message || err).slice(0, 160)}` };
                bytesToSave = arrayBuf;
            }
        }
        const saved = await saveMediaBytes(request, env, bytesToSave, {
            folder,
            filename: ".jpg",
            contentType: "image/jpeg",
            customMetadata: exifMeta && exifMeta.hasGeo !== false ? {
                gpsLat: String(exifMeta.lat || ""),
                gpsLng: String(exifMeta.lng || ""),
                city: String(exifMeta.city || ""),
                neighbourhood: String(exifMeta.neighbourhood || ""),
                exifGpsStamped: exif.stamped ? "true" : "false",
                exifGpsReason: exif.reason || ""
            } : undefined
        });
        return {
            ...saved,
            prompt,
            size: extra.size || requestedSize,
            quality: extra.quality || requestedQuality,
            geo: exifMeta && exifMeta.hasGeo !== false ? {
                lat: exifMeta.lat,
                lng: exifMeta.lng,
                city: exifMeta.city || "",
                neighbourhood: exifMeta.neighbourhood || "",
                source: exifMeta.source || "",
                exifGpsStamped: !!exif.stamped,
                exifGpsReason: exif.reason || ""
            } : null
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
                quality: "standard"
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
    const geo = resolveAgentGeo(profile || {}, body, index);
    const city = String(geo.city || profile.city || "").trim();
    const neighbourhoods = Array.isArray(profile.defaults?.photoNeighbourhoods) ?
        profile.defaults.photoNeighbourhoods :
        Array.isArray(profile.neighbourhoods) ? profile.neighbourhoods : [];
    const neighbourhood = String(geo.neighbourhood || neighbourhoods[index % Math.max(1, neighbourhoods.length)] || "").trim();
    if (body.prompt) {
        return buildEpfImageBrandingPrompt(body.prompt, {
            serviceType: service,
            city,
            businessName: profile.businessName || "EPF Pro Services"
        });
    }
    const serviceSignals = `${service} ${body.topic || ""} ${body.theme || ""}`.toLowerCase();
    const popcornFocus =
        serviceSignals.includes("popcorn") ||
        serviceSignals.includes("ceiling removal") ||
        serviceSignals.includes("stucco ceiling") ||
        serviceSignals.includes("ceiling texture");
    const headline = popcornFocus ? "Popcorn Ceiling Removal" : service;
    const locationLine = city ? city : "";
    const popcornScenes = [
        "top half shows a protected residential room during popcorn ceiling removal, ceiling partly textured and partly scraped smooth, contractor using a vacuum sander or scraper, plastic sheeting, blue masking tape, drop cloths, ladder, and natural window light",
        "top half shows a close upward angle of active popcorn ceiling removal with dust-control plastic, vacuum hose, pole sander, compound bucket, work light, and a clear transition from bumpy texture to smooth ceiling",
        "bottom half shows the finished transformed room after popcorn ceiling removal: smooth bright ceiling, clean wall edges, modern living room, recessed lights or natural window light, furniture uncovered, clean professional result",
        "split project-photo composition: removal prep and scraping above, smooth finished ceiling below, realistic contractor tools and ordinary residential details visible"
    ];
    const selectedPopcornScene = popcornScenes[index % popcornScenes.length];
    if (popcornFocus) {
        return buildEpfImageBrandingPrompt([
            "Create a square social media marketing image for a Google Business Profile post, in the same style as a professional contractor before-and-after service graphic.",
            "Use ultra-realistic human-shot residential renovation photography, not illustration, not 3D render, not glossy stock photography.",
            `Required service keyword text on the image, spelled exactly: "${headline}".`,
            locationLine
                ? `Add a second readable location line on the image, spelled exactly: "${locationLine}".`
                : "Do not invent a city; if no city is provided, use only the service keyword text.",
            `Business context: ${profile.businessName || "local contractor"}.`,
            `Location context: ${city}${neighbourhood ? `, ${neighbourhood}` : ""}.`,
            `Required visual scene: ${selectedPopcornScene}.`,
            "Layout: image should feel like a polished contractor post, with removal/prep photo area above, finished smooth-ceiling room below, and a clean centered banner overlay between them.",
            "Text treatment: large bold white service text on a dark navy rectangular banner; city line below on a warm beige or gold rectangle; high contrast; crisp readable lettering; no misspellings.",
            "Optional small Canadian maple leaf icon beside the city line only if it looks clean and does not hurt readability.",
            "Add a short premium tagline below the banner only if it is readable: CLEAN. MODERN. TRANSFORMED.",
            "Make popcorn ceiling removal instantly recognizable: bumpy/stucco texture, scraped smooth ceiling, dust protection, ladder, scraper or sander, vacuum hose, masking tape, compound bucket, and clean finished ceiling should be visible.",
            "Avoid fake logos, watermarks, business cards, random unreadable text, warped rooms, impossible tools, extra fingers, distorted ladders, clear customer faces, and overly perfect AI-looking surfaces."
        ].join(" "), {
            serviceType: headline,
            city: locationLine,
            businessName: profile.businessName || "EPF Pro Services"
        });
    }
    return buildEpfImageBrandingPrompt([
        "Create an ultra-realistic human-shot contractor project photo for Google Business Profile.",
        `Business: ${profile.businessName || "local contractor"}.`,
        `Service theme: ${service}.`,
        `Location context: ${city}${neighbourhood ? `, ${neighbourhood}` : ""}.`,
        "Make the service instantly recognizable beside Google search results.",
        "Use a real job-site look with ordinary residential surroundings, natural light, tools, dust protection, and realistic imperfections.",
        "Do not include text, logos, watermarks, readable labels, fake before/after text, distorted rooms, or stock-photo styling."
    ].join(" "), {
        serviceType: service,
        city,
        businessName: profile.businessName || "EPF Pro Services"
    });
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

    if (pathname.startsWith("/api/google/") || pathname.startsWith("/api/youtube/")) {
        const youtubeResponse = await handleYoutubeRoute(request, env, {
            jsonResponse,
            textResponse,
            parseJsonBody
        });
        if (youtubeResponse) return youtubeResponse;
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

    if (pathname === "/api/webhooks/blog-created/events") {
        return handleBlogWebhookEvents(request, env);
    }

    if (pathname === "/api/webhooks/blog-created/status") {
        return handleBlogAutomationStatus(request, env);
    }

    if (pathname === "/api/webhooks/blog-created") {
        return handleBlogCreatedWebhook(request, env);
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

            const exifMeta = profile ? buildAgentExifMeta(profile, body, 0) : buildAgentExifMeta({}, body, 0);
            let bytesToSave = bytes;
            let exif = { stamped: false, reason: "not_requested" };
            if (exifMeta.hasGeo && /jpe?g/i.test(mimeType || "")) {
                try {
                    exif = embedGpsExifInJpeg(bytes, exifMeta);
                    bytesToSave = exif.arrayBuffer;
                    if (exif.stamped) {
                        mimeType = "image/jpeg";
                        ext = ".jpg";
                    }
                } catch (err) {
                    console.warn("EXIF GPS stamping failed", err?.message || err);
                    exif = { stamped: false, reason: `stamp_failed: ${String(err?.message || err).slice(0, 160)}` };
                    bytesToSave = bytes;
                }
            }

            const saved = await saveMediaBytes(request, env, bytesToSave, {
                folder: body.folder || "ai",
                filename: exif.stamped ? ".jpg" : body.filename || ext,
                contentType: mimeType,
                customMetadata: exifMeta.hasGeo ? {
                    gpsLat: String(exifMeta.lat || ""),
                    gpsLng: String(exifMeta.lng || ""),
                    city: String(exifMeta.city || ""),
                    neighbourhood: String(exifMeta.neighbourhood || ""),
                    exifGpsStamped: exif.stamped ? "true" : "false",
                    exifGpsReason: exif.reason || ""
                } : undefined
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
                geo: exifMeta.hasGeo ? {
                    lat: exifMeta.lat,
                    lng: exifMeta.lng,
                    city: exifMeta.city || "",
                    neighbourhood: exifMeta.neighbourhood || "",
                    source: exifMeta.source || "",
                    exifGpsStamped: !!exif.stamped,
                    exifGpsReason: exif.reason || ""
                } : null,
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
                    folder: body.folder || "ai",
                    exifMeta: buildAgentExifMeta(profile, body, i)
                });
                generated.push(item);
            }
            if (body.addToProfile !== false) {
                await appendPhotosToProfile(env, profile.profileId, generated.map((item) => ({
                    url: item.url,
                    serviceType: body.serviceType || body.theme || body.topic || "",
                    serviceTopicId: body.serviceTopicId || "",
                    geo: item.geo || null
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
                    folder: body.folder || "ai",
                    exifMeta: buildAgentExifMeta(profile, body, 0)
                });
                mediaUrl = generated.url;
                if (body.addToProfile !== false) {
                    await appendPhotosToProfile(env, profile.profileId, [{
                        url: mediaUrl,
                        serviceType: body.serviceType || body.theme || body.topic || "",
                        serviceTopicId: body.serviceTopicId || "",
                        geo: generated?.geo || null
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
            const workId = crypto.randomUUID();
            const scheduleWork = async () => {
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
                            folder: body.folder || "ai",
                            exifMeta: buildAgentExifMeta(profile, body, i)
                        });
                        generated.push(item);
                        mediaUrl = item.url;
                    }

                    if (!mediaUrl) {
                        throw new Error("No mediaUrl available; provide mediaUrls or allow generateImages");
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
                        serviceTopicId: body.serviceTopicId || "",
                        geo: item.geo || null
                    })));
                }

                return { generated, scheduled };
            };

            const shouldRunInBackground =
                body.background !== false &&
                body.generateImages !== false &&
                count > 1 &&
                typeof ctx?.waitUntil === "function";

            if (shouldRunInBackground) {
                if (!body.photoOnly) {
                    const scheduled = [];
                    for (let i = 0; i < count; i++) {
                        const runAt = buildDailyRunAt(i, body);
                        // eslint-disable-next-line no-await-in-loop
                        const item = await enqueueScheduledPost(env, {
                            profileId: profile.profileId,
                            runAt,
                            body: {
                                ...body,
                                profileId: profile.profileId,
                                serviceType: body.serviceType || body.theme || body.topic || "",
                                autoGenerateSummary: body.autoGenerateSummary !== false,
                                mediaPending: true,
                                mediaGenerationWorkId: workId,
                                mediaGenerationIndex: i
                            }
                        });
                        scheduled.push(item);
                    }

                    ctx.waitUntil((async () => {
                        const generated = [];
                        for (let i = 0; i < scheduled.length; i++) {
                            const prompt = buildAgentImagePrompt(profile, body, i);
                            // eslint-disable-next-line no-await-in-loop
                            const image = await generateAiImageToGallery(request, env, {
                                ...body,
                                prompt,
                                folder: body.folder || "ai",
                                exifMeta: buildAgentExifMeta(profile, body, i)
                            });
                            generated.push(image);
                            const scheduledItem = scheduled[i];
                            // eslint-disable-next-line no-await-in-loop
                            await updateScheduledPost(env, scheduledItem.id, {
                                body: {
                                    ...scheduledItem.body,
                                    mediaUrl: image.url,
                                    mediaPending: false,
                                    generatedMediaKey: image.key,
                                    generatedMediaGeo: image.geo || null
                                }
                            });
                        }
                        if (generated.length && body.addToProfile !== false) {
                            await appendPhotosToProfile(env, profile.profileId, generated.map((item) => ({
                                url: item.url,
                                serviceType: body.serviceType || body.theme || body.topic || "",
                                serviceTopicId: body.serviceTopicId || "",
                                geo: item.geo || null
                            })));
                        }
                        console.log("agent daily schedule media background complete", {
                            workId,
                            profileId: profile.profileId,
                            generated: generated.length,
                            scheduled: scheduled.length
                        });
                    })().catch((err) => {
                        console.error("agent daily schedule media background failed", {
                            workId,
                            profileId: profile.profileId,
                            error: err?.message || String(err)
                        });
                    }));

                    return jsonResponse({
                        ok: true,
                        background: true,
                        workId,
                        profileId: profile.profileId,
                        businessName: profile.businessName || "",
                        mode: "post",
                        requestedCount: count,
                        scheduled,
                        message: "Posts were scheduled immediately. Image generation is continuing in the background and will attach media to each scheduled post when ready."
                    });
                }

                ctx.waitUntil(scheduleWork().then((result) => {
                    console.log("agent daily schedule background complete", {
                        workId,
                        profileId: profile.profileId,
                        generated: result.generated.length,
                        scheduled: result.scheduled.length
                    });
                }).catch((err) => {
                    console.error("agent daily schedule background failed", {
                        workId,
                        profileId: profile.profileId,
                        error: err?.message || String(err)
                    });
                }));
                return jsonResponse({
                    ok: true,
                    background: true,
                    workId,
                    profileId: profile.profileId,
                    businessName: profile.businessName || "",
                    mode: body.photoOnly ? "photo" : "post",
                    requestedCount: count,
                    message: "Image generation and scheduling started in the background. Check scheduled posts after a few minutes."
                });
            }

            const result = await scheduleWork();
            return jsonResponse({
                ok: true,
                background: false,
                workId,
                profileId: profile.profileId,
                businessName: profile.businessName || "",
                mode: body.photoOnly ? "photo" : "post",
                generated: result.generated,
                scheduled: result.scheduled
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
                        folder: body.folder || "ai",
                        exifMeta: buildAgentExifMeta(profile, body, i)
                    });
                    generated.push(item);
                }
                await appendPhotosToProfile(env, profile.profileId, generated.map((item) => ({
                    url: item.url,
                    serviceType: body.serviceType || body.theme || body.topic || "",
                    serviceTopicId: body.serviceTopicId || "",
                    geo: item.geo || null
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

    let m;

    // POST /profiles/:id/gbp-custom-services
    // Adds free-form GBP services to the Google Business Profile location.
    m = pathname.match(/^\/profiles\/([^/]+)\/gbp-custom-services$/);
    if (m && request.method === "POST") {
        const id = decodeURIComponent(m[1]);
        const body = await parseJsonBody(request);
        const profiles = await getProfiles(env);
        const profile = profiles.find((p) => p && p.profileId === id);
        if (!profile) return jsonResponse({ error: "Profile not found" }, 404);
        if (!profile.locationId) return jsonResponse({ error: "Profile is missing locationId" }, 400);

        const customServices = Array.isArray(body.services) && body.services.length ?
            body.services :
            CEILING_CUSTOM_GBP_SERVICES;
        const locationName = `locations/${profile.locationId}`;
        const getUrl =
            `https://mybusinessbusinessinformation.googleapis.com/v1/${locationName}` +
            `?readMask=${encodeURIComponent("name,languageCode,categories,serviceItems")}`;
        const location = await callBusinessProfileApi(env, getUrl);
        const category = String(
            body.category ||
            location?.categories?.primaryCategory?.name ||
            ""
        ).trim();
        if (!category) {
            return jsonResponse({ error: "GBP location is missing a primary category for custom services" }, 400);
        }

        const beforeItems = Array.isArray(location.serviceItems) ? location.serviceItems : [];
        const serviceItems = mergeFreeFormServiceItems(
            beforeItems,
            customServices,
            category,
            String(body.languageCode || location.languageCode || "en").trim() || "en"
        );
        const patchUrl =
            `https://mybusinessbusinessinformation.googleapis.com/v1/${locationName}` +
            `?updateMask=${encodeURIComponent("serviceItems")}`;
        const updated = await callBusinessProfileApi(env, patchUrl, {
            method: "PATCH",
            body: JSON.stringify({
                name: locationName,
                serviceItems
            })
        });

        return jsonResponse({
            ok: true,
            profileId: profile.profileId,
            businessName: profile.businessName || "",
            locationId: profile.locationId,
            category,
            beforeCount: beforeItems.length,
            afterCount: serviceItems.length,
            addedCount: serviceItems.length - beforeItems.length,
            services: serviceItems.map(getServiceItemName).filter(Boolean),
            updated
        });
    }

    // PATCH /profiles/:id/defaults
    m = pathname.match(/^\/profiles\/([^/]+)\/defaults$/);
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
                serviceTopicId: topic.id || "",
                linkUrl: topic.landingUrl || "",
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

    if (pathname.startsWith("/photo-scheduled/") && request.method === "PUT") {
        const id = decodeURIComponent(pathname.split("/").pop() || "");
        const body = await parseJsonBody(request);
        try {
            const item = await updateScheduledPhoto(env, id, body || {});
            return jsonResponse({ ok: true, item });
        } catch (e) {
            return jsonResponse({ error: e.message || "Update failed" }, 400);
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
        const prompt = buildEpfImageBrandingPrompt((body.prompt || "").trim() || "home renovation photo", {
            serviceType: body.serviceType || body.theme || body.topic || "",
            city: body.city || body.photoCity || "",
            businessName: body.businessName || "EPF Pro Services"
        });
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
                    quality: "standard"
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
            await runDueYoutubeScheduledJobs(env);
        } catch (e) {
            console.error("Scheduled error:", e);
        }
    }
};
