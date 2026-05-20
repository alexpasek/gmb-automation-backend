import { getYoutubeAccessToken } from "./youtubeAuth.mjs";

function parseYoutubeError(status, text) {
    if (/quota|rateLimit|dailyLimit|uploadLimit/i.test(text)) {
        return `YouTube quota/API error (${status}): ${text}`;
    }
    return `YouTube API error (${status}): ${text}`;
}

export async function uploadYoutubeVideo(env, channelRow, { file, title, description, tags, privacyStatus }) {
    if (!file || typeof file === "string") throw new Error("Missing video file");
    const accessToken = await getYoutubeAccessToken(env, channelRow);
    const metadata = {
        snippet: {
            title: String(title || "").slice(0, 100),
            description: String(description || ""),
            tags: Array.isArray(tags) ? tags.slice(0, 30) : [],
            categoryId: "26"
        },
        status: {
            privacyStatus: ["public", "unlisted", "private"].includes(privacyStatus) ? privacyStatus : "unlisted",
            selfDeclaredMadeForKids: false
        }
    };

    const initResp = await fetch("https://www.googleapis.com/upload/youtube/v3/videos?uploadType=resumable&part=snippet,status", {
        method: "POST",
        headers: {
            Authorization: `Bearer ${accessToken}`,
            "Content-Type": "application/json; charset=UTF-8",
            "X-Upload-Content-Type": file.type || "video/mp4"
        },
        body: JSON.stringify(metadata)
    });
    if (!initResp.ok) throw new Error(parseYoutubeError(initResp.status, await initResp.text()));
    const uploadUrl = initResp.headers.get("Location");
    if (!uploadUrl) throw new Error("YouTube did not return a resumable upload URL");

    const bytes = await file.arrayBuffer();
    const uploadResp = await fetch(uploadUrl, {
        method: "PUT",
        headers: {
            "Content-Type": file.type || "video/mp4",
            "Content-Length": String(bytes.byteLength)
        },
        body: bytes
    });
    if (!uploadResp.ok) throw new Error(parseYoutubeError(uploadResp.status, await uploadResp.text()));
    const data = await uploadResp.json();
    if (!data.id) throw new Error("YouTube upload completed without a video id");
    return {
        videoId: data.id,
        youtubeUrl: `https://www.youtube.com/watch?v=${data.id}`,
        raw: data
    };
}

export async function uploadYoutubeThumbnail(env, channelRow, videoId, file) {
    if (!videoId) throw new Error("Missing YouTube video id");
    if (!file || typeof file === "string") throw new Error("Missing thumbnail file");
    const accessToken = await getYoutubeAccessToken(env, channelRow);
    const bytes = await file.arrayBuffer();
    const resp = await fetch(`https://www.googleapis.com/upload/youtube/v3/thumbnails/set?videoId=${encodeURIComponent(videoId)}`, {
        method: "POST",
        headers: {
            Authorization: `Bearer ${accessToken}`,
            "Content-Type": file.type || "image/jpeg",
            "Content-Length": String(bytes.byteLength)
        },
        body: bytes
    });
    if (!resp.ok) throw new Error(parseYoutubeError(resp.status, await resp.text()));
    return resp.json();
}
