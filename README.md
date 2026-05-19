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
