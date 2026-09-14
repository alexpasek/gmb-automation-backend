const MAX_AGE = 600;

function cookieName(request, flow) {
    return `${new URL(request.url).protocol === "https:" ? "__Host-" : ""}google_oauth_${flow}`;
}

function stateCookie(request, flow, value, maxAge) {
    const secure = new URL(request.url).protocol === "https:" ? "; Secure" : "";
    return `${cookieName(request, flow)}=${value}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${maxAge}${secure}`;
}

export function startOAuth(request, flow, buildUrl) {
    const bytes = crypto.getRandomValues(new Uint8Array(32));
    const state = `${Date.now()}.${Array.from(bytes, b => b.toString(16).padStart(2, "0")).join("")}`;
    return new Response(null, {
        status: 302,
        headers: {
            Location: buildUrl(state),
            "Set-Cookie": stateCookie(request, flow, state, MAX_AGE),
            "Cache-Control": "no-store"
        }
    });
}

export function validOAuthState(request, flow) {
    const state = new URL(request.url).searchParams.get("state") || "";
    const prefix = `${cookieName(request, flow)}=`;
    const cookie = (request.headers.get("Cookie") || "").split(";")
        .map(part => part.trim()).find(part => part.startsWith(prefix));
    if (!/^\d+\.[a-f0-9]{64}$/.test(state) || !cookie || cookie.slice(prefix.length) !== state) return false;
    const age = Date.now() - Number(state.split(".")[0]);
    return age >= 0 && age <= MAX_AGE * 1000;
}

export function oauthCallbackHeaders(request, flow) {
    return {
        "Content-Type": "text/plain",
        "Cache-Control": "no-store",
        "Referrer-Policy": "no-referrer",
        "Set-Cookie": stateCookie(request, flow, "", 0)
    };
}
