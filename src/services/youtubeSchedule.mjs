import { postToGmb } from "../gmb.mjs";
import { getYoutubeChannelForUse, insertYoutubeCommunityDraft, insertYoutubeVideoRecord, updateYoutubeVideoRecord } from "./youtubeAuth.mjs";
import { generateYoutubeCommunityPost, generateYoutubeSeo } from "./youtubeSeo.mjs";
import { uploadYoutubeThumbnail, uploadYoutubeVideo } from "./youtubeUpload.mjs";

function nowIso() {
    return new Date().toISOString();
}

function randomId(prefix = "ytsched") {
    if (typeof crypto !== "undefined" && crypto.randomUUID) return `${prefix}-${crypto.randomUUID()}`;
    return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
}

function parseJson(text, fallback) {
    try { return JSON.parse(text || ""); } catch { return fallback; }
}

export async function ensureYoutubeScheduleTable(env) {
    await env.D1_DB.prepare(`
      CREATE TABLE IF NOT EXISTS youtube_scheduled_jobs (
        id TEXT PRIMARY KEY,
        job_type TEXT NOT NULL,
        channel_db_id TEXT,
        channel_id TEXT,
        profile_id TEXT,
        run_at TEXT NOT NULL,
        status TEXT NOT NULL,
        payload TEXT NOT NULL,
        video_r2_key TEXT,
        video_content_type TEXT,
        thumbnail_r2_key TEXT,
        thumbnail_content_type TEXT,
        result TEXT,
        error_message TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );
    `).run();
}

export async function createYoutubeScheduledJob(env, payload = {}) {
    await ensureYoutubeScheduleTable(env);
    const runAt = new Date(payload.runAt);
    if (!payload.jobType) throw new Error("Missing scheduled job type");
    if (!runAt || Number.isNaN(runAt.getTime())) throw new Error("Invalid scheduled run time");
    const id = randomId();
    const now = nowIso();
    await env.D1_DB.prepare(`
      INSERT INTO youtube_scheduled_jobs (
        id, job_type, channel_db_id, channel_id, profile_id, run_at, status, payload,
        video_r2_key, video_content_type, thumbnail_r2_key, thumbnail_content_type,
        result, error_message, created_at, updated_at
      )
      VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'QUEUED', ?7, ?8, ?9, ?10, ?11, '', '', ?12, ?12)
    `).bind(
        id,
        payload.jobType,
        payload.channelDbId || "",
        payload.channelId || "",
        payload.profileId || "",
        runAt.toISOString(),
        JSON.stringify(payload.payload || {}),
        payload.videoR2Key || "",
        payload.videoContentType || "",
        payload.thumbnailR2Key || "",
        payload.thumbnailContentType || "",
        now
    ).run();
    return { id, status: "QUEUED", runAt: runAt.toISOString() };
}

export async function listYoutubeScheduledJobs(env, limit = 50) {
    await ensureYoutubeScheduleTable(env);
    const safeLimit = Math.max(1, Math.min(100, parseInt(limit, 10) || 50));
    const { results } = await env.D1_DB.prepare(`
      SELECT *
      FROM youtube_scheduled_jobs
      ORDER BY run_at DESC
      LIMIT ?1
    `).bind(safeLimit).all();
    return (results || []).map((row) => ({
        ...row,
        payload: parseJson(row.payload, {}),
        result: parseJson(row.result, null)
    }));
}

async function updateJob(env, id, patch = {}) {
    const row = await env.D1_DB.prepare("SELECT * FROM youtube_scheduled_jobs WHERE id = ?1").bind(id).first();
    if (!row) return null;
    const next = { ...row, ...patch };
    await env.D1_DB.prepare(`
      UPDATE youtube_scheduled_jobs
      SET status = ?2, result = ?3, error_message = ?4, updated_at = ?5
      WHERE id = ?1
    `).bind(
        id,
        next.status || row.status,
        typeof next.result === "string" ? next.result : JSON.stringify(next.result || ""),
        next.error_message || next.errorMessage || "",
        nowIso()
    ).run();
    return next;
}

async function r2File(env, key, contentType) {
    if (!key) return null;
    const obj = await env.MEDIA_BUCKET.get(key);
    if (!obj) throw new Error(`Scheduled media missing in R2: ${key}`);
    return {
        type: contentType || obj.httpMetadata?.contentType || "application/octet-stream",
        size: obj.size || 0,
        arrayBuffer: () => obj.arrayBuffer()
    };
}

async function processVideoJob(env, job) {
    const payload = parseJson(job.payload, {});
    const channel = await getYoutubeChannelForUse(env, job.channel_db_id || job.channel_id);
    const seo = payload.seo || generateYoutubeSeo(payload);
    const recordId = await insertYoutubeVideoRecord(env, {
        channelDbId: channel.id,
        channelId: channel.channel_id,
        title: seo.title,
        description: seo.description,
        tags: seo.tags,
        hashtags: seo.hashtags,
        service: payload.service || "",
        city: payload.city || "",
        neighbourhoods: payload.neighbourhoods || [],
        videoType: payload.videoType || "",
        websiteUrl: seo.websiteUrl || payload.landingPageUrl || "",
        utmUrl: seo.utmUrl || "",
        privacyStatus: payload.privacyStatus || "unlisted",
        publishStatus: "SCHEDULED_UPLOADING"
    });
    const videoFile = await r2File(env, job.video_r2_key, job.video_content_type || "video/mp4");
    const uploaded = await uploadYoutubeVideo(env, channel, {
        file: videoFile,
        title: seo.title,
        description: seo.description,
        tags: seo.tags,
        privacyStatus: payload.privacyStatus || "unlisted"
    });
    let thumbnailStatus = "";
    if (job.thumbnail_r2_key) {
        try {
            const thumb = await r2File(env, job.thumbnail_r2_key, job.thumbnail_content_type || "image/jpeg");
            await uploadYoutubeThumbnail(env, channel, uploaded.videoId, thumb);
            thumbnailStatus = "UPLOADED";
        } catch (e) {
            thumbnailStatus = "FAILED: " + (e?.message || String(e));
        }
    }
    let gbpCrossPostStatus = "";
    let gbpResult = null;
    if (payload.crossPostGbp && job.profile_id) {
        try {
            gbpResult = await postToGmb(env, {
                profileId: job.profile_id,
                postText: `${seo.gbpCrossPostText}\n\n${uploaded.youtubeUrl}`,
                cta: "LEARN_MORE",
                linkUrl: uploaded.youtubeUrl,
                serviceType: payload.service || ""
            });
            gbpCrossPostStatus = "POSTED";
        } catch (e) {
            gbpCrossPostStatus = "FAILED: " + (e?.message || String(e));
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
    return { recordId, video: uploaded, thumbnailStatus, gbpCrossPostStatus };
}

async function processCommunityJob(env, job) {
    const payload = parseJson(job.payload, {});
    const channel = job.channel_db_id || job.channel_id ? await getYoutubeChannelForUse(env, job.channel_db_id || job.channel_id) : null;
    const generated = payload.postText ? null : generateYoutubeCommunityPost(payload);
    const draftId = await insertYoutubeCommunityDraft(env, {
        channelDbId: channel?.id || "",
        channelId: channel?.channel_id || job.channel_id || "",
        postText: payload.postText || generated.postText,
        imageUrl: payload.imageUrl || "",
        service: payload.service || "",
        city: payload.city || "",
        neighbourhoods: payload.neighbourhoods || [],
        postType: payload.postType || "community_post",
        websiteUrl: payload.websiteUrl || generated?.websiteUrl || "",
        utmUrl: payload.utmUrl || generated?.utmUrl || "",
        hashtags: payload.hashtags || generated?.hashtags || [],
        status: "READY"
    });
    return { draftId, manualPublishRequired: true };
}

export async function runDueYoutubeScheduledJobs(env, limit = 3) {
    await ensureYoutubeScheduleTable(env);
    const { results } = await env.D1_DB.prepare(`
      SELECT *
      FROM youtube_scheduled_jobs
      WHERE status = 'QUEUED' AND run_at <= ?1
      ORDER BY run_at ASC
      LIMIT ?2
    `).bind(nowIso(), Math.max(1, Math.min(10, limit || 3))).all();
    const processed = [];
    for (const job of results || []) {
        try {
            await updateJob(env, job.id, { status: "RUNNING" });
            const result = job.job_type === "VIDEO_UPLOAD" ?
                await processVideoJob(env, job) :
                await processCommunityJob(env, job);
            await updateJob(env, job.id, { status: "DONE", result });
            processed.push({ id: job.id, ok: true, result });
        } catch (e) {
            const error = e?.message || String(e);
            await updateJob(env, job.id, { status: "FAILED", errorMessage: error });
            processed.push({ id: job.id, ok: false, error });
        }
    }
    return { ok: true, processed };
}
