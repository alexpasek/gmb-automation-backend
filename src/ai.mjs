export function pickNeighbourhood(profile) {
    const p = profile || {};
    const city = typeof p.city === "string" ? p.city : "";
    const cityLower = city.toLowerCase();
    const arr = Array.isArray(p.neighbourhoods) ? p.neighbourhoods : [];

    const calgaryDefaults = [
        "NW Calgary",
        "SW Calgary",
        "SE Calgary",
        "NE Calgary",
        "Beltline",
        "Bridgeland",
        "Kensington",
        "Inglewood",
        "Marda Loop",
        "Mission",
        "Altadore",
        "Mount Royal",
        "Killarney",
        "West Springs",
        "Aspen Woods",
        "Lake Bonavista",
        "Mahogany",
        "Seton",
        "Sage Hill",
        "Evanston",
        "Auburn Bay",
        "Varsity",
        "Dalhousie",
        "Brentwood",
        "Crescent Heights",
        "Ramsay",
        "Renfrew",
        "Signal Hill",
        "Cougar Ridge"
    ];

    const merged = [];
    const seen = new Set();
    const pushIfNew = (val) => {
        const v = String(val || "").trim();
        if (!v || seen.has(v.toLowerCase())) return;
        seen.add(v.toLowerCase());
        merged.push(v);
    };

    arr.forEach(pushIfNew);
    if (cityLower.includes("calgary")) {
        calgaryDefaults.forEach(pushIfNew);
    }

    if (!merged.length) return city;
    const useCityOnly = Math.random() < 0.5 && city; // bias to keep Calgary visible
    if (useCityOnly) return city;
    const idx = Math.floor(Math.random() * merged.length);
    return merged[idx];
}

export function safeJoinHashtags(arr, maxChars) {
    if (!Array.isArray(arr)) return "";
    let out = "";
    for (let i = 0; i < arr.length; i++) {
        let h = String(arr[i] || "").trim();
        if (!h) continue;
        if (h[0] !== "#") h = "#" + h.replace(/^#+/, "");
        const candidate = out === "" ? h : out + " " + h;
        if (candidate.length > maxChars) break;
        out = candidate;
    }
    return out;
}

function parseJsonResponse(text) {
    let s = String(text || "");
    if (s.indexOf("```") !== -1) {
        const first = s.indexOf("{");
        const last = s.lastIndexOf("}");
        if (first !== -1 && last !== -1 && last > first) {
            s = s.slice(first, last + 1);
        }
    }
    try {
        const obj = JSON.parse(s);
        if (obj && typeof obj === "object") return obj;
        return null;
    } catch {
        return null;
    }
}

export async function aiGenerateSummaryAndHashtags(env, profile, neighbourhood) {
    const city = (profile && profile.city) || "";
    const businessName = (profile && profile.businessName) || "";
    const keywords = Array.isArray(profile && profile.keywords) ?
        profile.keywords : [];
    const kwLine = keywords.join(", ");

    const area = neighbourhood || "";
    const whereOptions = area ? [
        area + ", " + city,
        "the " + area + " area of " + city,
        area + " in " + city,
        city + " — including " + area,
        area + " and nearby " + city,
        area + " / " + city
    ] : [city];
    const where =
        whereOptions[Math.floor(Math.random() * whereOptions.length)] || city;

    const tones = [
        "friendly and helpful",
        "confident and professional",
        "warm and community-focused",
        "concise and action-oriented",
        "benefit-driven and practical",

        "calm and reassuring",
        "straightforward and honest",
        "upbeat and encouraging",
        "relaxed and conversational",
        "respectful and down-to-earth",
        "detail-oriented and precise",
        "patient and understanding",
        "energetic and motivating",
        "caring and service-minded",
        "humble and hardworking",
        "solution-focused and optimistic",
        "trustworthy and transparent",
        "neighbourly and familiar",
        "expert yet approachable",
        "safety-focused and responsible",
        "family-friendly and welcoming",
        "modern and forward-thinking",
        "organised and reliable",
        "flexible and easygoing",
        "calm under pressure",
        "positive and can-do",
        "thoughtful and empathetic",
        "meticulous and quality-focused",
        "efficient and no-nonsense",
        "premium yet accessible",
        "friendly but no-pressure",
        "clear and educational",
        "proactive and check-in often",
        "respectful of your time",
        "respectful of your budget",
        "soft-spoken and polite",
        "bold and confident",
        "creative and design-minded",
        "technical but easy to follow",
        "generous and value-focused",
        "collaborative and team-focused",
        "service-first and client-led",
        "calm, slow, and careful",
        "fast, sharp, and decisive",
        "courteous and professional",
        "playful and light-hearted",
        "results-focused and metrics-minded",
        "supportive and coaching-style",
        "local and neighbourhood-minded",
        "high-end and boutique-style",
    ];
    const tone = tones[Math.floor(Math.random() * tones.length)];

    const angles = [
        "Highlight dust control and tidy clean-up after every visit.",
        "Emphasise colour matching, prep work, and smooth finishes.",
        "Spotlight fast turnaround with clear communication at each stage.",
        "Mention reviews, trust, and showing up on time for neighbours.",
        "Focus on before/after transformations and photo-worthy results.",

        "Explain how we protect furniture, floors, and personal items.",
        "Talk about walking the customer through the plan before starting.",
        "Highlight using premium, low-VOC materials that are safer indoors.",
        "Emphasise respectful crews who remove shoes and clean as they go.",
        "Share stories of fixing “nightmare” jobs left by other contractors.",
        "Focus on clear quotes with no surprise add-ons or hidden fees.",
        "Mention flexible scheduling around work, kids, and pets.",
        "Highlight punctual arrival and sticking to promised timelines.",
        "Explain how we handle small touch-ups even after the job is done.",
        "Talk about sending progress photos and updates during longer projects.",
        "Emphasise careful masking, taping, and surface protection.",
        "Highlight satisfaction guarantees and coming back if something’s missed.",
        "Talk about honest advice, even if it means a smaller project.",
        "Focus on long-lasting results that still look good years later.",
        "Mention friendly follow-ups to make sure everything feels right.",
        "Emphasise respect for noise levels, neighbours, and building rules.",
        "Highlight collaboration on colours, finishes, and design choices.",
        "Talk about solving common home problems, not just doing a job.",
        "Explain how we minimise mess for families working from home.",
        "Mention tidy workspaces, organised tools, and labelled materials.",
        "Highlight being fully insured and following best safety practices.",
        "Focus on helping homeowners feel proud to invite guests again.",
        "Share tips for caring for new finishes after the project.",
        "Emphasise staying on budget and warning early about any changes.",
        "Highlight being reachable by phone, text, or email with quick replies.",
        "Talk about respecting cultural and personal preferences in the home.",
        "Mention protecting kids’ and pets’ areas with extra care.",
        "Focus on small details that make the room feel finished.",
        "Highlight coordination with other trades when needed.",
        "Emphasise minimal disruption so life can continue during the project.",
        "Talk about clear start and finish times each day.",
        "Highlight showing up prepared with everything needed on day one.",
        "Mention walking the space together at the end for a final check.",
        "Focus on turning builder-basic spaces into upgraded, modern rooms.",
        "Share how we help get homes ready for sale or staging.",
        "Emphasise experience with older homes and tricky repairs.",
        "Highlight fair pricing that reflects craftsmanship and reliability.",
        "Talk about custom solutions instead of one-size-fits-all packages.",
        "Mention real local photos instead of stock imagery.",
        "Focus on building long-term relationships, not one-time jobs.",
        "Highlight simple, plain-language explanations instead of jargon.",
        "Talk about being honest if something is not worth doing yet.",
        "Emphasise starting on time, finishing on time, and cleaning up.",
        "Highlight that we treat every home as if it were our own.",
        "Focus on leaving the space fresh, calm, and ready to enjoy.",
    ];
    const angle = angles[Math.floor(Math.random() * angles.length)];

    const openers = [
        "Kick off with a vivid local hook using the neighbourhood name.",
        "Open with a concise promise plus the neighbourhood mention.",
        "Start with the main service and immediately anchor to the area.",
        "Lead with a quick win or micro-case in the neighbourhood.",
        "Open with how neighbours describe the experience.",

        "Begin with a question about a common home issue in this neighbourhood.",
        "Start by painting a before-and-after picture right on this street.",
        "Lead with a real-life scenario a local homeowner would recognise.",
        "Open by acknowledging how much pride locals take in their homes.",
        "Start with a short, relatable story about a nearby project.",
        "Kick off by mentioning a familiar local landmark or main road.",
        "Open with how it feels to walk into a freshly updated local home.",
        "Start with a simple ‘imagine this’ moment tied to the neighbourhood.",
        "Lead with a quick problem-solution sentence tied to local homes.",
        "Open by highlighting how long we’ve been working in this area.",
        "Begin with a neighbour’s challenge and how it was resolved.",
        "Start with a small detail only locals would notice or care about.",
        "Open with a seasonal angle specific to this part of the city.",
        "Lead with a question about comfort, light, or noise in local homes.",
        "Begin by contrasting old finishes with the modern look locals want.",
        "Open by acknowledging how busy life is in this neighbourhood.",
        "Start with a simple promise to make their home feel better to live in.",
        "Lead with a line about turning ‘builder-basic’ into something special.",
        "Begin by referencing how the area is changing and upgrading.",
        "Open with a quick nod to families, kids, or pets in local homes.",
        "Start by mentioning how many neighbours have already upgraded.",
        "Lead with the most common complaint we hear from locals.",
        "Open with a gentle call-out to a pain point people rarely talk about.",
        "Begin with a short testimonial-style line in a neighbour’s voice.",
        "Start with a bold statement about what we *don’t* do (no mess, no delays).",
        "Lead with a ‘you’re not the only one’ reassurance for local homeowners.",
        "Open by connecting the service to local property values and resale.",
        "Start with a quick stat or number about recent projects nearby.",
        "Begin with how we make projects easier for busy local families.",
        "Open with a line about protecting floors, furniture, and keepsakes.",
        "Lead with a focus on trust: who shows up, when, and how.",
        "Start with a one-line story: problem, action, result, all local.",
        "Open by inviting the reader to picture their home one week from now.",
        "Begin with how the space feels in the morning or evening in this area.",
        "Lead with a simple yes/no question about something they see every day.",
        "Open with a phrase locals would actually use about their ceilings or walls.",
        "Start by calling out a small detail that quietly bothers most neighbours.",
        "Begin with a transformation theme: ‘from dated to fresh’ in this neighbourhood.",
        "Open with a line about making guests say ‘wow’ when they walk in.",
        "Lead with how we keep projects predictable: time, budget, and mess.",
        "Start by referencing parking, access, or condo rules locals deal with.",
        "Open with a calm, reassuring line about working in lived-in homes.",
        "Begin with a simple, friendly ‘here’s what we’re doing for your neighbours.’",
        "Start with a micro-case: one room, one problem, one great outcome in this area.",
    ];

    const opener = openers[Math.floor(Math.random() * openers.length)];

    const ctas = [
        "Request a free quote today.",
        "Get your free estimate now.",
        "Message us for a free estimate.",
        "Book a free quote today.",
        "Contact us for a no-obligation quote."
    ];
    const ctaLine = ctas[Math.floor(Math.random() * ctas.length)];

    const prompt =
        "Return ONLY valid JSON with fields: summary (string), hashtags (array of 5-7 strings). " +
        "Do not include markdown fences. " +
        "Constraints: summary 80-120 words, " +
        tone +
        ", no phone numbers, no emojis in body, no hashtags in body. " +
        'Write as this business speaking in first person plural ("we"). ' +
        "Opener rule: " +
        opener +
        " Always mention the neighbourhood or city in the first two sentences. " +
        "Angle for uniqueness: " +
        angle +
        " Include at least one main service keyword like 'popcorn ceiling removal', 'drywall repair', or 'interior painting'. " +
        "Include one concrete detail (timeline, material, or measurable benefit) to avoid generic phrasing. " +
        "Mention the location naturally (city and neighbourhood) in the body; do not repeat the exact same phrasing each time. " +
        "Highlight a trust factor: clean work, dust control, before/after results, or reviews. " +
        "End the body with EXACTLY this CTA: " +
        ctaLine +
        " " +
        "Hashtags should be concise, readable, and include a mix of general and geo hashtags (no punctuation except '#').\n\n" +
        "Business: " +
        businessName +
        "\n" +
        "City/Area: " +
        where +
        "\n" +
        "Keywords to inspire (do not list verbatim): " +
        kwLine +
        "\n";

    const body = {
        model: "gpt-4o-mini",
        messages: [{ role: "user", content: prompt }],
        temperature: 0.9
    };

    const resp = await fetch("https://api.openai.com/v1/chat/completions", {
        method: "POST",
        headers: {
            Authorization: "Bearer " + env.OPENAI_API_KEY,
            "Content-Type": "application/json"
        },
        body: JSON.stringify(body)
    });

    if (!resp.ok) {
        const text = await resp.text();
        throw new Error("OpenAI error: " + text);
    }

    const data = await resp.json();
    const txt =
        (data &&
            data.choices &&
            data.choices[0] &&
            data.choices[0].message &&
            data.choices[0].message.content) ||
        "";
    const obj = parseJsonResponse(txt);
    if (!obj) {
        return { summary: String(txt || "").trim(), hashtags: [] };
    }

    let summary =
        typeof obj.summary === "string" ? obj.summary.trim() : String(txt || "");
    let hashtags = Array.isArray(obj.hashtags) ? obj.hashtags : [];
    const cleaned = [];
    for (let i = 0; i < hashtags.length; i++) {
        let h = String(hashtags[i] || "").trim();
        if (!h) continue;
        if (h[0] !== "#") h = "#" + h.replace(/^#+/, "");
        cleaned.push(h);
    }
    hashtags = cleaned;

    return { summary, hashtags };
}
