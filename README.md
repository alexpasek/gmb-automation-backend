# GMB Automation Backend (Cloudflare Workers + D1)

This is a Cloudflare Worker backend that posts to Google Business Profile (GMB) using AI-generated copy and a scheduler.

## Setup

1. Install dependencies:

```bash
cd backend
npm install
```

2. Create a D1 database:

```bash
npx wrangler d1 create gmb_automation
```

Copy the resulting `database_id` into `wrangler.toml` under `[[d1_databases]]`.

3. Create the `kv` table in the D1 database:

```sql
CREATE TABLE kv (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
```

4. Configure secrets:

```bash
npx wrangler secret put OPENAI_API_KEY
npx wrangler secret put GOOGLE_CLIENT_ID
npx wrangler secret put GOOGLE_CLIENT_SECRET
npx wrangler secret put GOOGLE_REDIRECT_URI
npx wrangler secret put EPF_WEBHOOK_SECRET
```

For `GOOGLE_REDIRECT_URI`, use your Worker URL, e.g.:

`https://gmb-automation-backend.<your-subdomain>.workers.dev/oauth2callback`

5. Seed profiles

Insert your existing `profiles.json` into the D1 KV:

```json
{ "key": "profiles", "value": "[ ... your profiles array ... ]" }
```

You can do this via the Cloudflare dashboard or using `wrangler d1 execute`.

6. Run locally:

```bash
npm run dev
```

7. Deploy:

```bash
npm run deploy
```

## R2 uploads

The Worker’s `/upload` route writes to an R2 bucket bound as `MEDIA_BUCKET` and serves files from `/media/<key>`.

Add the binding in `wrangler.toml` (already present):

```toml
[[r2_buckets]]
binding = "MEDIA_BUCKET"
bucket_name = "gmb-media" # change to your bucket name
```

Create the bucket in your Cloudflare account (or update `bucket_name` to an existing one), then deploy:

```bash
npm run deploy
```

## EPF blog webhook

The website can notify the GMB poster when a blog post is published:

`POST /api/webhooks/blog-created`

Required header:

`x-epf-webhook-secret: <EPF_WEBHOOK_SECRET>`

Expected JSON:

```json
{
  "event": "BLOG_POST_CREATED",
  "url": "https://epfproservices.com/blog/popcorn-ceiling-removal-burlington/",
  "title": "Popcorn Ceiling Removal in Burlington",
  "excerpt": "Short blog description here",
  "city": "Burlington",
  "service": "Popcorn Ceiling Removal",
  "publishedAt": "2026-06-12T18:00:00Z"
}
```

Current workflow:

1. Validates the secret and payload.
2. Accepts the EPF blog URL from the webhook payload. The webhook does not send Google, local, Maps, or GBP post URLs.
3. Stores the blog event in D1-backed KV.
4. Rejects duplicate blog URLs.
5. Routes EPF blog posts to the EPF popcorn profile, except Hamilton/Stoney Creek posts, which route to the Stoney Creek/Hamilton location.
6. Generates GBP post copy.
7. Derives the city service page for the GBP Learn More button.
8. Includes the original blog URL in the post body.
9. Adds rotating local SEO signals from GBP address/service-area data plus city postal-code hints.
10. Publishes the post to the matched Google Business Profile automatically.
11. Stores `POSTED` or `POST_FAILED` status with the event.

For example, if the webhook sends only:

`https://epfproservices.com/blog/popcorn-removal-oakville-cost-finish-guide/`

the poster agent can derive:

- Learn More URL: `https://epfproservices.com/popcorn-ceiling-removal/oakville/`
- Blog article line: the original webhook blog URL
- Google/local/maps links: generated later by the GMB poster system, not supplied by the webhook

There is no manual approval step in this flow.

Saved webhook events can be checked with:

`GET /api/webhooks/blog-created/events`

Use the same `x-epf-webhook-secret` header.

Dashboard-safe automation status is available at:

`GET /api/webhooks/blog-created/status`

## Agent API for GPT Actions

Set an agent token before exposing these routes:

```bash
npx wrangler secret put AGENT_API_KEY
```

Agent calls must send either `Authorization: Bearer <AGENT_API_KEY>` or `x-agent-api-key: <AGENT_API_KEY>`.

Main routes:

- `GET /agent/profiles` - list profiles the agent can target.
- `GET /agent/gallery?folder=ai` - list AI gallery files from R2.
- `POST /agent/gallery/upload` - upload `imageUrl`, `imageBase64`, or `dataUrl` into the AI gallery.
- `POST /agent/images/generate` - generate one or more images, save them to R2 `ai/`, and optionally add them to a profile photo pool.
- `POST /agent/posts/now` - generate or use an image, then post now as a GBP post or photo-only upload.
- `POST /agent/posts/daily` - generate/use images and queue one post per day.
- `POST /agent/scheduler/enable` - enable the recurring profile scheduler and optionally prefill the profile photo pool with AI images.

Example: generate daily popcorn ceiling posts for Mississauga:

```json
{
  "city": "Mississauga",
  "serviceType": "popcorn ceiling removal",
  "imageCount": 14,
  "time": "10:00",
  "generateImages": true,
  "cadence": "DAILY1"
}
```

Import `agent-openapi.json` into your GPT Action configuration and set API key authentication with the same token.
