# Automatic website blog delivery

The existing five-minute Cloudflare cron checks `https://epfproservices.com/blog/` when `BLOG_MONITOR_ENABLED=true`. It discovers same-origin article links and fetches each new live article's title and description before calling the existing authenticated publishing handler. That handler selects the business profile, generates local post copy and a photo, and publishes to Google.

The first successful scan records all existing URLs as a baseline without publishing them. Later scans process at most one new article per tick. The monitor retains seen URLs even if they disappear from the index. Only articles linked from the live blog index are discovered.

D1 `kv` key `blogMonitor` stores the baseline, check time, pending count and latest result. `GET /api/webhooks/blog-created/status` exposes operational status without exposing the webhook secret or the full baseline. The app's Blog Automation page displays these results.

D1 `blog_deliveries` stores a permanent unique URL claim shared by cron and incoming webhooks. A failed or interrupted publishing attempt is held for review because Google may have accepted a request before a connection failed. Website-fetch errors retry on the next tick. Inspect Google and the event/history records before manually releasing any delivery claim. This intentionally favors avoiding duplicate public posts over blindly retrying an ambiguous request.

Existing webhook authentication and manual posting remain available. Disabling `BLOG_MONITOR_ENABLED` stops website polling without changing other scheduled jobs.

Validation: `node --test test/*.test.mjs`; frontend `node scripts/check-workspace.cjs` and production build. Discovery and publishing behavior are tested with mocked external delivery; no historical articles are published as a test.
