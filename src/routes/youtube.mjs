import { postToGmb } from "../gmb.mjs";
import { buildYoutubeAuthUrl, getYoutubeChannelForUse, insertYoutubeCommunityDraft, insertYoutubeVideoRecord, listYoutubeChannels, listYoutubeCommunityDrafts, markYoutubeCommunityDraftPosted, saveYoutubeConnection, updateYoutubeVideoRecord } from "../services/youtubeAuth.mjs";
import { generateYoutubeCommunityPost, generateYoutubeSeo } from "../services/youtubeSeo.mjs";
import { uploadYoutubeThumbnail, uploadYoutubeVideo } from "../services/youtubeUpload.mjs";
import { createYoutubeScheduledJob, listYoutubeScheduledJobs, runDueYoutubeScheduledJobs } from "../services/youtubeSchedule.mjs";

function splitNeighbourhoods(value) {
    if (Array.isArray(value)) return value.map((x) => String(x || "").trim()).filter(Boolean);
    return String(value || "").split(/[,;\n]+/).map((x) => x.trim()).filter(Boolean);
}

function formString(form, key, fallback = "") {
    const value = form.get(key);
    return typeof value === "string" ? value : fallback;
}

function safeJson(value, fallback) {
    try {
        return JSON.parse(value);
    } catch {
        return fallback;
    }
}

function guessImageType(file) {
    const type = String(file?.type || "").trim();
    if (type) return type;
    const name = String(file?.name || "").toLowerCase();
    if (name.endsWith(".png")) return "image/png";
    if (name.endsWith(".webp")) return "image/webp";
    return "image/jpeg";
}

async function saveCommunityImage(request, env, file) {
    if (!file || typeof file === "string" || !file.size) return "";
    const type = guessImageType(file);
    const ext =
        type.includes("png") ? ".png" :
        type.includes("webp") ? ".webp" :
        ".jpg";
    const key = `gmb/youtube-community/${Date.now()}-${Math.random().toString(36).slice(2)}${ext}`;
    await env.MEDIA_BUCKET.put(key, await file.arrayBuffer(), {
        httpMetadata: { contentType: type }
    });
    return `${new URL(request.url).origin}/media/${encodeURIComponent(key)}`;
}

function guessVideoType(file) {
    const type = String(file?.type || "").trim();
    return type || "video/mp4";
}

async function saveScheduledMedia(env, file, folder, fallbackType) {
    if (!file || typeof file === "string" || !file.size) return { key: "", contentType: "" };
    const contentType = file.type || fallbackType || "application/octet-stream";
    const name = String(file.name || "");
    const extMatch = name.match(/\.[a-zA-Z0-9]+$/);
    const ext = extMatch ? extMatch[0].toLowerCase() : "";
    const key = `gmb/${folder}/${Date.now()}-${Math.random().toString(36).slice(2)}${ext}`;
    await env.MEDIA_BUCKET.put(key, await file.arrayBuffer(), {
        httpMetadata: { contentType }
    });
    return { key, contentType };
}

export async function handleYoutubeRoute(request, env, helpers = {}) {
    const url = new URL(request.url);
    const { pathname, searchParams } = url;
    const jsonResponse = helpers.jsonResponse;
    const textResponse = helpers.textResponse;
    const parseJsonBody = helpers.parseJsonBody;

    try {
        if (pathname === "/api/google/connect-youtube" && request.method === "GET") {
            return Response.redirect(buildYoutubeAuthUrl(request, env), 302);
        }

        if (pathname === "/api/google/oauth/callback" && request.method === "GET") {
            const error = searchParams.get("error");
            if (error) return textResponse(`Google OAuth error: ${error}`, 400, { "Content-Type": "text/plain" });
            const code = searchParams.get("code");
            if (!code) return textResponse("Missing Google OAuth code.", 400, { "Content-Type": "text/plain" });
            const channel = await saveYoutubeConnection(request, env, code);
            return textResponse(`YouTube channel connected: ${channel.channelTitle}. You can close this tab.`, 200, { "Content-Type": "text/plain" });
        }

        if (pathname === "/api/youtube/channels" && request.method === "GET") {
            return jsonResponse({ channels: await listYoutubeChannels(env) });
        }

        if (pathname === "/api/youtube/generate-seo" && request.method === "POST") {
            const body = await parseJsonBody(request);
            return jsonResponse({ ok: true, seo: generateYoutubeSeo(body) });
        }

        if (pathname === "/api/youtube/community/generate" && request.method === "POST") {
            const body = await parseJsonBody(request);
            return jsonResponse({ ok: true, communityPost: generateYoutubeCommunityPost(body) });
        }

        if (pathname === "/api/youtube/community/drafts" && request.method === "GET") {
            const limit = searchParams.get("limit") || "50";
            return jsonResponse({ drafts: await listYoutubeCommunityDrafts(env, limit) });
        }

        if (pathname === "/api/youtube/scheduled" && request.method === "GET") {
            return jsonResponse({ jobs: await listYoutubeScheduledJobs(env, searchParams.get("limit") || "50") });
        }

        if (pathname === "/api/youtube/scheduled/run-due" && request.method === "POST") {
            return jsonResponse(await runDueYoutubeScheduledJobs(env, 5));
        }

        if (pathname === "/api/youtube/schedule" && request.method === "POST") {
            const contentType = request.headers.get("Content-Type") || "";
            let payload = {};
            let videoFile = null;
            let thumbnailFile = null;
            if (contentType.toLowerCase().includes("multipart/form-data")) {
                const form = await request.formData();
                payload = {
                    jobType: formString(form, "jobType", "VIDEO_UPLOAD"),
                    channelId: formString(form, "channelId"),
                    profileId: formString(form, "profileId"),
                    runAt: formString(form, "runAt"),
                    service: formString(form, "service"),
                    city: formString(form, "city"),
                    neighbourhoods: splitNeighbourhoods(formString(form, "neighbourhoods")),
                    videoType: formString(form, "videoType"),
                    postType: formString(form, "postType") || formString(form, "videoType"),
                    landingPageUrl: formString(form, "landingPageUrl") || formString(form, "websiteUrl"),
                    websiteUrl: formString(form, "websiteUrl") || formString(form, "landingPageUrl"),
                    privacyStatus: formString(form, "privacyStatus", "unlisted"),
                    crossPostGbp: formString(form, "crossPostGbp") === "true",
                    postText: formString(form, "postText"),
                    imageUrl: formString(form, "imageUrl"),
                    seo: safeJson(formString(form, "seoJson", "null"), null)
                };
                videoFile = form.get("video");
                thumbnailFile = form.get("thumbnail");
            } else {
                payload = await parseJsonBody(request);
            }

            const channel = payload.channelId ? await getYoutubeChannelForUse(env, payload.channelId) : null;
            let videoMedia = { key: "", contentType: "" };
            let thumbMedia = { key: "", contentType: "" };
            if (payload.jobType === "VIDEO_UPLOAD") {
                if (!videoFile || typeof videoFile === "string") return jsonResponse({ error: "Missing video file for scheduled upload" }, 400);
                videoMedia = await saveScheduledMedia(env, videoFile, "youtube-scheduled/videos", guessVideoType(videoFile));
                thumbMedia = await saveScheduledMedia(env, thumbnailFile, "youtube-scheduled/thumbnails", guessImageType(thumbnailFile));
            }
            const job = await createYoutubeScheduledJob(env, {
                jobType: payload.jobType === "COMMUNITY_DRAFT" ? "COMMUNITY_DRAFT" : "VIDEO_UPLOAD",
                channelDbId: channel?.id || "",
                channelId: channel?.channel_id || payload.channelId || "",
                profileId: payload.profileId || "",
                runAt: payload.runAt,
                payload,
                videoR2Key: videoMedia.key,
                videoContentType: videoMedia.contentType,
                thumbnailR2Key: thumbMedia.key,
                thumbnailContentType: thumbMedia.contentType
            });
            return jsonResponse({ ok: true, job });
        }

        if (pathname === "/api/youtube/community/drafts" && request.method === "POST") {
            const contentType = request.headers.get("Content-Type") || "";
            let payload = {};
            let imageFile = null;
            if (contentType.toLowerCase().includes("multipart/form-data")) {
                const form = await request.formData();
                payload = {
                    channelId: formString(form, "channelId"),
                    service: formString(form, "service"),
                    city: formString(form, "city"),
                    neighbourhoods: splitNeighbourhoods(formString(form, "neighbourhoods")),
                    postType: formString(form, "postType", "community_post"),
                    websiteUrl: formString(form, "websiteUrl") || formString(form, "landingPageUrl"),
                    utmUrl: formString(form, "utmUrl"),
                    postText: formString(form, "postText"),
                    imageUrl: formString(form, "imageUrl"),
                    hashtags: safeJson(formString(form, "hashtags", "[]"), [])
                };
                imageFile = form.get("image");
            } else {
                payload = await parseJsonBody(request);
            }

            const channel = payload.channelId ? await getYoutubeChannelForUse(env, payload.channelId) : null;
            const generated = payload.postText ? null : generateYoutubeCommunityPost(payload);
            const postText = payload.postText || generated.postText;
            const imageUrl = payload.imageUrl || await saveCommunityImage(request, env, imageFile);
            const draftId = await insertYoutubeCommunityDraft(env, {
                channelDbId: channel?.id || "",
                channelId: channel?.channel_id || payload.channelId || "",
                postText,
                imageUrl,
                service: payload.service || "",
                city: payload.city || "",
                neighbourhoods: splitNeighbourhoods(payload.neighbourhoods),
                postType: payload.postType || generated?.postType || "community_post",
                websiteUrl: payload.websiteUrl || generated?.websiteUrl || "",
                utmUrl: payload.utmUrl || generated?.utmUrl || "",
                hashtags: payload.hashtags || generated?.hashtags || [],
                status: "DRAFT"
            });
            return jsonResponse({ ok: true, draftId, postText, imageUrl });
        }

        if (pathname === "/api/youtube/upload" && request.method === "POST") {
            const form = await request.formData();
            const channelId = formString(form, "channelId");
            const service = formString(form, "service");
            const city = formString(form, "city");
            const videoType = formString(form, "videoType");
            const websiteUrl = formString(form, "landingPageUrl") || formString(form, "websiteUrl");
            const privacyStatus = formString(form, "privacyStatus", "unlisted");
            const profileId = formString(form, "profileId");
            const crossPostGbp = formString(form, "crossPostGbp") === "true";
            const neighbourhoods = splitNeighbourhoods(formString(form, "neighbourhoods"));
            const seoOverride = safeJson(formString(form, "seoJson", "{}"), {});
            const videoFile = form.get("video");
            const thumbnailFile = form.get("thumbnail");
            if (!channelId) return jsonResponse({ error: "Missing YouTube channel" }, 400);
            if (!videoFile || typeof videoFile === "string") return jsonResponse({ error: "Missing video file" }, 400);

            const channel = await getYoutubeChannelForUse(env, channelId);
            const seo = {
                ...generateYoutubeSeo({ service, city, videoType, landingPageUrl: websiteUrl, neighbourhoods }),
                ...seoOverride
            };
            const recordId = await insertYoutubeVideoRecord(env, {
                channelDbId: channel.id,
                channelId: channel.channel_id,
                title: seo.title,
                description: seo.description,
                tags: seo.tags,
                hashtags: seo.hashtags,
                service,
                city,
                neighbourhoods,
                videoType,
                websiteUrl: seo.websiteUrl || websiteUrl,
                utmUrl: seo.utmUrl,
                privacyStatus,
                publishStatus: "UPLOADING"
            });

            try {
                const uploaded = await uploadYoutubeVideo(env, channel, {
                    file: videoFile,
                    title: seo.title,
                    description: seo.description,
                    tags: seo.tags,
                    privacyStatus
                });
                let thumbnailStatus = "";
                let thumbnailResult = null;
                if (thumbnailFile && typeof thumbnailFile !== "string" && thumbnailFile.size > 0) {
                    try {
                        thumbnailResult = await uploadYoutubeThumbnail(env, channel, uploaded.videoId, thumbnailFile);
                        thumbnailStatus = "UPLOADED";
                    } catch (thumbErr) {
                        thumbnailStatus = "FAILED: " + (thumbErr?.message || String(thumbErr));
                    }
                }

                let gbpResult = null;
                let gbpCrossPostStatus = crossPostGbp ? "SKIPPED" : "";
                if (crossPostGbp) {
                    if (!profileId) {
                        gbpCrossPostStatus = "FAILED: Missing GBP profile";
                    } else {
                        try {
                            gbpResult = await postToGmb(env, {
                                profileId,
                                postText: `${seo.gbpCrossPostText}\n\n${uploaded.youtubeUrl}`,
                                cta: "LEARN_MORE",
                                linkUrl: uploaded.youtubeUrl,
                                serviceType: service
                            });
                            gbpCrossPostStatus = "POSTED";
                        } catch (gbpErr) {
                            gbpCrossPostStatus = "FAILED: " + (gbpErr?.message || String(gbpErr));
                        }
                    }
                }

                await updateYoutubeVideoRecord(env, recordId, {
                    youtubeVideoId: uploaded.videoId,
                    youtubeUrl: uploaded.youtubeUrl,
                    publishStatus: "UPLOADED",
                    thumbnailStatus,
                    gbpCrossPostStatus,
                    gbpResult
                });
                return jsonResponse({ ok: true, recordId, video: uploaded, seo, thumbnailStatus, thumbnailResult, gbpCrossPostStatus, gbpResult });
            } catch (uploadErr) {
                const message = uploadErr?.message || String(uploadErr);
                await updateYoutubeVideoRecord(env, recordId, {
                    publishStatus: "FAILED",
                    errorMessage: message
                });
                return jsonResponse({ error: message, recordId }, 502);
            }
        }

        if (pathname === "/api/youtube/thumbnail" && request.method === "POST") {
            const form = await request.formData();
            const channelId = formString(form, "channelId");
            const videoId = formString(form, "videoId");
            const file = form.get("thumbnail");
            const channel = await getYoutubeChannelForUse(env, channelId);
            const result = await uploadYoutubeThumbnail(env, channel, videoId, file);
            return jsonResponse({ ok: true, result });
        }

        if (pathname === "/api/youtube/cross-post-gbp" && request.method === "POST") {
            const body = await parseJsonBody(request);
            if (!body.profileId) return jsonResponse({ error: "Missing profileId" }, 400);
            const result = await postToGmb(env, {
                profileId: body.profileId,
                postText: body.postText || body.gbpCrossPostText || "",
                cta: "LEARN_MORE",
                linkUrl: body.youtubeUrl || body.utmUrl || body.websiteUrl || "",
                serviceType: body.service || ""
            });
            return jsonResponse({ ok: true, result });
        }

        const markMatch = pathname.match(/^\/api\/youtube\/community\/drafts\/([^/]+)\/mark-posted$/);
        if (markMatch && request.method === "POST") {
            const draft = await markYoutubeCommunityDraftPosted(env, decodeURIComponent(markMatch[1]));
            return jsonResponse({ ok: true, draft });
        }

        return null;
    } catch (e) {
        const message = e?.message || String(e);
        const status = /Missing|Invalid|reconnect|required/i.test(message) ? 400 : 500;
        return jsonResponse({ error: message }, status);
    }
}
