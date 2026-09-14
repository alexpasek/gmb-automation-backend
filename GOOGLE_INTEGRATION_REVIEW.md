# Google integration review — 2026-09-14

Reviewed the app's Google Business Profile and YouTube integration against current official documentation. The reviewed changes were deployed on September 14, 2026. Live read-only checks passed for accounts, locations, performance, profiles, schedules, YouTube connections, and history. No new Google posts were created during deployment verification.

## Necessary fixes

- `/accounts` now calls the Account Management API instead of the Business Information API's nonexistent accounts-list endpoint. The response shape is unchanged.
- GBP and YouTube authorization now use random, browser-bound OAuth state with a ten-minute lifetime, separate cookies for each integration, and callback validation before exchanging codes. HTTPS cookies use Secure, HttpOnly, SameSite=Lax and the __Host- prefix. Existing stored connections do not require reconnection for this change. Start new connections and receive callbacks on the same backend host.
- Removed logging of GBP tokens, authorization codes, and callback query strings.
- Expired GBP credentials produce a useful reconnect error instead of silently reusing an expired token after refresh failure. Temporary refresh failures preserve stored credentials.
- Temporary YouTube refresh failures no longer permanently flag a channel for reconnection. Google's invalid_grant response still requires reconnection.
- Keyword performance follows nextPageToken instead of silently stopping at 25 keywords. Existing metric calculations and response fields are preserved.

## Compatibility findings

The current daily performance method, Business Information v1 location calls, GBP v4 local-post/media endpoints, and YouTube v3 upload/thumbnail endpoints remain documented. No replacement of posting, scheduling, profile synchronization, photo workflows, or UI was needed. Recent optional review fields and recurring-post features do not require migration of this app's existing scheduler. The discontinued Q&A API is not used by the reviewed integration.

## Validation

- `node --test test/google-integration.test.mjs`: nine regression tests passed using mocked Google requests and D1 storage.
- Backend Wrangler deployment dry run produced a Worker bundle.
- Frontend production build passed.
- `git diff --check` passed.

Live Google consent, actual account access, API enablement/quotas, and production posting have not been tested. Tests do not make external calls or modify production data.

## Official references

- [Google Business Profile change log](https://developers.google.com/my-business/content/change-log)
- [Deprecation schedule](https://developers.google.com/my-business/content/sunset-dates)
- [Account Management accounts.list](https://developers.google.com/my-business/reference/accountmanagement/rest/v1/accounts/list)
- [Google OAuth web-server flow](https://developers.google.com/identity/protocols/oauth2/web-server)
- [Keyword impressions pagination](https://developers.google.com/my-business/reference/performance/rest/v1/locations.searchkeywords.impressions.monthly/list)
- [GBP media creation](https://developers.google.com/my-business/reference/rest/v4/accounts.locations.media/create)
- [YouTube thumbnail upload and accepted scopes](https://developers.google.com/youtube/v3/docs/thumbnails/set)
