function slugPart(value) {
    return String(value || "")
        .trim()
        .toLowerCase()
        .replace(/&/g, "and")
        .replace(/[^a-z0-9]+/g, "_")
        .replace(/^_+|_+$/g, "");
}

export function buildYoutubeUtmUrl(websiteUrl, { city, service, videoType, medium = "video" } = {}) {
    const raw = String(websiteUrl || "").trim();
    if (!/^https?:\/\//i.test(raw)) {
        throw new Error("Landing page URL must start with http:// or https://");
    }

    const url = new URL(raw);
    url.searchParams.set("utm_source", "youtube");
    url.searchParams.set("utm_medium", medium || "video");
    url.searchParams.set("utm_campaign", `${slugPart(city)}_${slugPart(service)}`.replace(/^_+|_+$/g, ""));
    url.searchParams.set("utm_content", slugPart(videoType));
    return url.toString();
}
