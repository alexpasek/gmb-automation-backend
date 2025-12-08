// export-profiles-from-locations.mjs
//
// 1. Reads locations from your local backend
// 2. Converts them to the profile objects your app expects
// 3. Prints JSON you can paste into PROFILES_JSON on Cloudflare

const ACCOUNT_ID = "116118369255335894193"; // your accountId
const BASE = "http://127.0.0.1:8787";

async function main() {
    const url = `${BASE}/locations?accountId=${ACCOUNT_ID}`;
    const resp = await fetch(url);
    if (!resp.ok) {
        console.error("Failed to fetch /locations:", resp.status, await resp.text());
        process.exit(1);
    }

    const data = await resp.json();
    const locations = Array.isArray(data.locations) ? data.locations : [];

    const profiles = locations.map((loc, idx) => {
        const name = String(loc.name || "");
        const locId = name.split("/").pop() || `loc-${idx + 1}`;
        const businessName = String(loc.title || `Location ${idx + 1}`);

        const address = loc.storefrontAddress || {};
        const city =
            address.locality ||
            address.postalCode ||
            address.administrativeArea ||
            "";

        return {
            profileId: locId, // internal ID used by UI & scheduler
            businessName, // required
            accountId: ACCOUNT_ID, // required
            locationId: locId, // required (Google locationId)
            city, // optional, nice for AI + UI
            neighbourhoods: [], // you can edit later in JSON if you want
            keywords: [], // same
            photoPool: [], // same
            disabled: false,
            enableSiteCTA: false,
            landingUrl: loc.websiteUri || "" // website, if present
        };
    });

    console.log(JSON.stringify(profiles, null, 2));
}

main().catch((e) => {
    console.error(e);
    process.exit(1);
});