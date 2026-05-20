import { postToGmb } from "../gmb.mjs";
import { buildYoutubeAuthUrl, getYoutubeChannelForUse, insertYoutubeVideoRecord, listYoutubeChannels, saveYoutubeConnection, updateYoutubeVideoRecord } from "../services/youtubeAuth.mjs";
import { generateYoutubeSeo } from "../services/youtubeSeo.mjs";
import { uploadYoutubeThumbnail, uploadYoutubeVideo } from "../services/youtubeUpload.mjs";

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

        return null;
    } catch (e) {
        const message = e?.message || String(e);
        const status = /Missing|Invalid|reconnect|required/i.test(message) ? 400 : 500;
        return jsonResponse({ error: message }, status);
    }
}
