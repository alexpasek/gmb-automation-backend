import { buildYoutubeUtmUrl } from "./utmBuilder.mjs";

const BUSINESS_NAME = "EPF Pro Services";

function clean(value) {
    return String(value || "").trim();
}

function titleCase(value) {
    return clean(value)
        .split(/\s+/)
        .filter(Boolean)
        .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
        .join(" ");
}

function toHashtag(value) {
    const tag = clean(value).replace(/[^a-zA-Z0-9]+/g, "");
    return tag ? `#${tag}` : "";
}

function uniqueList(items, max = 20) {
    const seen = new Set();
    const out = [];
    for (const item of items) {
        const value = clean(item);
        const key = value.toLowerCase();
        if (!value || seen.has(key)) continue;
        seen.add(key);
        out.push(value);
        if (out.length >= max) break;
    }
    return out;
}

function splitNeighbourhoods(value) {
    if (Array.isArray(value)) return value.map(clean).filter(Boolean);
    return clean(value)
        .split(/[,;\n]+/)
        .map(clean)
        .filter(Boolean);
}

function serviceExplanation(service) {
    const lower = clean(service).toLowerCase();
    if (lower.includes("popcorn")) {
        return "Our crew protects the home, removes and preps the texture, skim coats where needed, sands, primes, paints, and cleans up so the ceiling looks smooth and finished.";
    }
    if (lower.includes("drywall install")) {
        return "We handle clean drywall installation with careful boarding, finishing prep, and practical scheduling for residential and light commercial spaces.";
    }
    if (lower.includes("drywall repair")) {
        return "We repair damaged drywall, blend surfaces, and prepare the area for a clean paint-ready finish.";
    }
    if (lower.includes("baseboard")) {
        return "We install trim with careful measuring, tight joints, caulking, and finish-ready detail.";
    }
    return `We help local property owners plan and complete ${clean(service).toLowerCase()} with clear communication and tidy workmanship.`;
}

export function generateYoutubeSeo(input = {}) {
    const service = clean(input.service);
    const city = clean(input.city);
    const videoType = clean(input.videoType || "project video");
    const landingPageUrl = clean(input.landingPageUrl || input.websiteUrl);
    const neighbourhoods = splitNeighbourhoods(input.neighbourhoods);

    if (!service) throw new Error("Missing service");
    if (!city) throw new Error("Missing city");
    if (!landingPageUrl) throw new Error("Missing landing page URL");

    const utmUrl = buildYoutubeUtmUrl(landingPageUrl, { city, service, videoType });
    const isShort = /short/i.test(videoType);
    const locationText = neighbourhoods.length ?
        `${city}, including nearby ${neighbourhoods.slice(0, 4).join(", ")}` :
        city;
    const title = isShort ?
        `${titleCase(service)} in ${titleCase(city)}`.slice(0, 70) :
        `${titleCase(service)} in ${titleCase(city)} | ${BUSINESS_NAME}`;

    const descriptionParts = [
        `${BUSINESS_NAME} provides ${service.toLowerCase()} in ${locationText}.`,
        serviceExplanation(service),
        `This ${videoType.toLowerCase()} focuses on one local service area so homeowners can understand what to expect before booking.`,
        `Learn more or request an estimate: ${utmUrl}`,
        `Call ${BUSINESS_NAME} to plan your ${service.toLowerCase()} project in ${city}.`
    ];

    const hashtags = uniqueList([
        toHashtag(service),
        toHashtag(city),
        "#EPFProServices",
        "#HomeImprovement",
        "#LocalContractor"
    ], 6);

    const tags = uniqueList([
        service,
        `${service} ${city}`,
        `${city} ${service}`,
        BUSINESS_NAME,
        ...neighbourhoods.map((n) => `${service} ${n}`),
        "home improvement",
        "local contractor"
    ], 18);

    const gbpCrossPostText = [
        `${BUSINESS_NAME} just posted a new YouTube video about ${service.toLowerCase()} in ${city}.`,
        neighbourhoods.length ? `We also serve nearby areas like ${neighbourhoods.slice(0, 3).join(", ")}.` : "",
        `Watch it here:`,
        utmUrl
    ].filter(Boolean).join("\n\n");

    return {
        businessName: BUSINESS_NAME,
        title,
        description: `${descriptionParts.join("\n\n")}\n\n${hashtags.join(" ")}`,
        tags,
        hashtags,
        thumbnailTextIdea: isShort ? `${titleCase(service)}\n${titleCase(city)}` : `${titleCase(city)} ${titleCase(service)}\nBefore You Book`,
        gbpCrossPostText,
        websiteUrl: landingPageUrl,
        utmUrl
    };
}
