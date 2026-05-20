import { buildYoutubeUtmUrl } from "./utmBuilder.mjs";

const BUSINESS_NAME = "EPF Pro Services";
const DEFAULT_PHONE = "(647) 923-6784";

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

function processBullets(service) {
    const lower = clean(service).toLowerCase();
    if (lower.includes("popcorn") || lower.includes("ceiling")) {
        return [
            "floor and furniture protection before messy work starts",
            "careful texture removal and surface preparation",
            "skim coating or drywall finishing where the ceiling needs it",
            "sanding, priming, painting, and final cleanup"
        ];
    }
    if (lower.includes("drywall")) {
        return [
            "protecting the work area",
            "clean board installation or damaged-area repair",
            "taping, mudding, sanding, and finish preparation",
            "paint-ready cleanup before the room goes back to normal"
        ];
    }
    return [
        "clear project preparation",
        "practical scheduling and communication",
        "careful workmanship for the room or surface",
        "cleanup before the job is considered finished"
    ];
}

function usefulViewerNotes(service, city, neighbourhoods) {
    const lower = clean(service).toLowerCase();
    const area = neighbourhoods.length ? `, including ${neighbourhoods.slice(0, 3).join(", ")}` : "";
    if (lower.includes("popcorn")) {
        return [
            `If you are comparing popcorn ceiling removal in ${city}${area}, look at more than the scraping step. The result depends on protection, surface prep, skim coating decisions, sanding, primer, paint, and cleanup.`,
            "Older ceilings can hide uneven drywall or previous repairs, so a smooth finish usually needs a plan before the first scrape."
        ];
    }
    if (lower.includes("drywall")) {
        return [
            `For ${service.toLowerCase()} in ${city}${area}, the important part is making the repair or installation disappear once light hits the wall.`,
            "Good drywall work is usually quiet in the final room: straight lines, smooth transitions, and a surface that is ready for paint."
        ];
    }
    return [
        `For ${service.toLowerCase()} in ${city}${area}, the best results come from clear prep, realistic timing, and a finish that matches the rest of the home.`,
        "This video is meant to help local homeowners understand the work before requesting an estimate."
    ];
}

function buildHumanDescription({ service, city, neighbourhoods, videoType, utmUrl }) {
    const notes = usefulViewerNotes(service, city, neighbourhoods);
    const process = processBullets(service).map((item) => `- ${item}`).join("\n");
    const nearby = neighbourhoods.length ?
        `We also work with homeowners in nearby areas such as ${neighbourhoods.slice(0, 5).join(", ")}.` :
        "";

    return [
        `${BUSINESS_NAME} helps homeowners with ${service.toLowerCase()} in ${city}.`,
        notes.join("\n\n"),
        nearby,
        `In this ${videoType.toLowerCase()}, we focus on what matters before booking: what the work involves, what affects the final finish, and how to plan the next step with less guesswork.`,
        `Our process can include:\n${process}`,
        `Request a free estimate or see the most relevant landing page:\n${utmUrl}`,
        `Call ${BUSINESS_NAME} when you want a cleaner, better-planned ${service.toLowerCase()} project in ${city}.`
    ].filter(Boolean).join("\n\n");
}

export function generateYoutubeCommunityPost(input = {}) {
    const service = clean(input.service);
    const city = clean(input.city);
    const postType = clean(input.postType || input.videoType || "community_post");
    const landingPageUrl = clean(input.landingPageUrl || input.websiteUrl);
    const phone = clean(input.phone || DEFAULT_PHONE);
    const website = clean(input.website || landingPageUrl);
    const neighbourhoods = splitNeighbourhoods(input.neighbourhoods);

    if (!service) throw new Error("Missing service");
    if (!city) throw new Error("Missing city");
    if (!landingPageUrl) throw new Error("Missing landing page URL");

    const utmUrl = buildYoutubeUtmUrl(landingPageUrl, {
        city,
        service,
        videoType: postType,
        medium: "community_post"
    });
    const hashtags = uniqueList([
        toHashtag(service),
        toHashtag(city),
        "#SmoothCeilings",
        "#EPFProServices",
        "#HomeImprovement"
    ], 6);
    const nearbyLine = neighbourhoods.length ?
        `We also serve nearby areas like ${neighbourhoods.slice(0, 4).join(", ")}.` :
        "";
    const process = processBullets(service).map((item) => `- ${item}`).join("\n");
    const text = [
        `${titleCase(service)} in ${titleCase(city)}`,
        `${BUSINESS_NAME} helps homeowners with ${service.toLowerCase()} in ${city} and nearby areas.`,
        nearbyLine,
        "Our process can include:",
        process,
        BUSINESS_NAME,
        `${city}, ON`,
        phone,
        website,
        "Request a free estimate:",
        utmUrl,
        hashtags.join(" ")
    ].filter(Boolean).join("\n\n");

    return {
        businessName: BUSINESS_NAME,
        postText: text,
        hashtags,
        websiteUrl: landingPageUrl,
        utmUrl,
        postType
    };
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

    const description = buildHumanDescription({ service, city, neighbourhoods, videoType, utmUrl });

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
        `${BUSINESS_NAME} shared a new video for homeowners planning ${service.toLowerCase()} in ${city}.`,
        neighbourhoods.length ? `It may also help if you are nearby in ${neighbourhoods.slice(0, 3).join(", ")}.` : "",
        serviceExplanation(service),
        "Watch the video or request an estimate here:",
        utmUrl
    ].filter(Boolean).join("\n\n");
    const communityPost = generateYoutubeCommunityPost({
        service,
        city,
        neighbourhoods,
        landingPageUrl,
        postType: videoType
    });

    return {
        businessName: BUSINESS_NAME,
        title,
        description: `${description}\n\n${hashtags.join(" ")}`,
        tags,
        hashtags,
        thumbnailTextIdea: isShort ? `${titleCase(service)}\n${titleCase(city)}` : `${titleCase(city)} ${titleCase(service)}\nBefore You Book`,
        gbpCrossPostText,
        communityPostText: communityPost.postText,
        communityPostUtmUrl: communityPost.utmUrl,
        websiteUrl: landingPageUrl,
        utmUrl
    };
}
