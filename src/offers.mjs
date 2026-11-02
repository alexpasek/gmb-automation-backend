// Google LocalPost: event is required for OFFER; callToAction is ignored.
// https://developers.google.com/my-business/reference/rest/v4/accounts.locations.localPosts
export class OfferValidationError extends Error {
    constructor(message) { super(message); this.status = 400; }
}

function fail(message) { throw new OfferValidationError(message); }
function text(value) { return String(value || "").trim(); }
function dateParts(value, label) {
    const s = text(value);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) fail(`${label} is required (YYYY-MM-DD).`);
    const d = new Date(`${s}T00:00:00Z`);
    if (!Number.isFinite(d.getTime()) || d.toISOString().slice(0, 10) !== s) fail(`${label} must be a valid calendar date.`);
    return { year: Number(s.slice(0, 4)), month: Number(s.slice(5, 7)), day: Number(s.slice(8, 10)) };
}

export function buildOfferFields(body = {}) {
    const title = text(body.offerTitle);
    if (!title || title.length > 58) fail("Offer title is required and must be 58 characters or fewer.");
    const start = text(body.offerStart), end = text(body.offerEnd);
    const startDate = dateParts(start, "Offer start date"), endDate = dateParts(end, "Offer end date");
    if (end < start) fail("Offer end date must be on or after its start date.");
    const couponCode = text(body.offerCoupon), redeemOnlineUrl = text(body.offerRedeemUrl), termsConditions = text(body.offerTerms);
    if (redeemOnlineUrl) {
        let url;
        try { url = new URL(redeemOnlineUrl); } catch { fail("Offer redemption link must be an http:// or https:// URL."); }
        if (!["https:", "http:"].includes(url.protocol) || url.username || url.password) fail("Offer redemption link must be an http:// or https:// URL without credentials.");
    }
    return {
        event: { title, schedule: { startDate, endDate } },
        offer: {
            ...(couponCode ? { couponCode } : {}),
            ...(redeemOnlineUrl ? { redeemOnlineUrl } : {}),
            ...(termsConditions ? { termsConditions } : {})
        }
    };
}

export function validateOffer(body = {}, runAt = null, now = new Date()) {
    if (body.topicType !== "OFFER") return;
    buildOfferFields(body);
    if (!text(body.postText)) fail("Write the offer description before publishing or scheduling.");
    // The app's business locations use Toronto local dates, including DST.
    const day = d => new Intl.DateTimeFormat("en-CA", { timeZone: "America/Toronto", year: "numeric", month: "2-digit", day: "2-digit" }).format(d);
    if (body.offerEnd < day(now)) fail("Offer has already expired. Choose a current or future end date.");
    if (runAt) {
        const when = new Date(runAt);
        if (!Number.isFinite(when.getTime())) fail("Offer publish date is invalid.");
        if (body.offerEnd < day(when)) fail("Offer must be published before its end date has passed.");
    }
}
