# ESA Web Attribution — Server-Side Diagnostic Report

**Date:** 2026-04-27
**Investigation:** Why query-string params (UTMs, gclid) appeared to be stripped on `https://extraspaceasia.com/sg/promotions/?utm_source=test&utm_campaign=manualcheck&gclid=ABC123`

---

## TL;DR

**The origin server does NOT strip query strings on `/sg/` pages.** All 35 server-side tests against `extraspaceasia.com/sg/promotions/` and other `/sg/*` paths returned `200 OK` with params preserved. The `307 Temporary Redirect` you saw in your browser session is environment-specific — it does not reproduce from a clean unauthenticated request.

There is **one** confirmed bad redirect (site root → `/sg/` strips params), but it does not affect `/sg/promotions/` or `/sg/get-started/` directly.

---

## Stack Detected

| Layer | Component |
|---|---|
| Web server | nginx 1.28.2 |
| Origin IP | 18.143.27.110 (AWS Singapore) |
| CMS | WordPress |
| Cache plugin | WP Rocket (visible via `data-minify="1"` and `wp-content/cache/min/2/`) |
| Localization | Polylang (very likely — site has `/sg/`, `/my/`, `/kr/`, `/hk/` markets) |
| GTM container | `GTM-W76SVC9Q` |
| Forms | Contact Form 7 + ReCAPTCHA + country-phone-field |
| Other plugins | wp-whatsapp, 3d-flipbook-dflip-lite |

---

## Test Matrix

### Server-side redirect behavior on /sg/ paths

| URL | Status | Location header | Params preserved |
|---|---|---|---|
| `/sg/?utm_source=test&gclid=ABC123` | 200 OK | — | ✅ |
| `/sg/promotions/?utm_source=test&gclid=ABC123` | 200 OK | — | ✅ |
| `/sg/get-started/?utm_source=test&gclid=ABC123` | 200 OK | — | ✅ |
| `/sg/get-started/?utm_source=google&utm_medium=cpc&utm_campaign=brand&gclid=Cj0KCQjwabc123` | 200 OK | — | ✅ |
| `/sg/promotions/` (no params, control) | 200 OK | — | n/a |

### Redirects that DO occur (and how they handle params)

| Source | Status | Destination | Params preserved? |
|---|---|---|---|
| `https://extraspaceasia.com/?utm_source=test&gclid=ABC123` | 302 | `https://extraspaceasia.com/sg/` | ❌ **DROPPED** |
| `https://extraspaceasia.com?utm_source=test&gclid=ABC123` | 302 | `https://extraspaceasia.com/sg/` | ❌ **DROPPED** |
| `https://www.extraspaceasia.com/sg/promotions/?utm_source=test&gclid=ABC123` | 301 | `https://extraspaceasia.com/sg/promotions/?utm_source=test&gclid=ABC123` | ✅ |
| `/sg/promotions?utm_source=test&gclid=ABC123` (no trailing slash) | 301 | `/sg/promotions/?utm_source=test&gclid=ABC123` | ✅ |
| `/sg/about/?utm_source=test&gclid=ABC123` | 404 | — | n/a |
| `/jp/?utm_source=test&gclid=ABC123` | 404 | — | n/a |
| `/tw/?utm_source=test&gclid=ABC123` | 404 | — | n/a |

The only confirmed param-dropping redirect is the **bare domain root → `/sg/`**, set by WordPress (response includes `X-Redirect-By: WordPress`). Probably a Polylang default-language rule.

### Conditions that did NOT reproduce the 307

I could not trigger a 307 on `/sg/promotions/?utm_source=test&gclid=ABC123` under any of these conditions:

- Different User-Agents (curl default, Chrome 147 desktop)
- Full Chrome browser headers including all `Sec-Fetch-*`, `Cache-Control: no-cache`, `Pragma: no-cache`
- Cookies: `wp-settings-1`, `pll_language=en/zh/ja/ko/th/zh_CN`, `cookie_notice_accepted=true`
- Geo-IP signal headers: `CF-IPCountry: SG/US/MY/KR`
- HTTP/1.1 vs HTTP/2
- HEAD vs GET method
- `Referer: https://www.google.com/aclk?...` (simulating an ad click)

### Different param names — all worked

| Param | Status |
|---|---|
| `?utm_source=test` | 200 |
| `?gclid=ABC123` | 200 |
| `?fbclid=XYZ789` | 200 |
| `?foo=bar` | 200 |
| `?ref=newsletter` | 200 |
| Multi-param combos | 200 |

So the (hypothetical) redirect rule isn't keyed on any specific marketing param name we tested.

---

## Why You Saw a 307 But I Cannot Reproduce It

The 307 in your Network tab is conditional on something specific to your session. Most likely candidates, in order:

### 1. WP Rocket cache served a stale entry
WP Rocket is on the site. Cache plugins sometimes cache the *response* of a URL keyed by query string, and if a redirect was once written to that cache key, every subsequent visit replays the redirect. **Curl misses cache because no cache cookies / no Vary match → fresh hit → 200.**

How to verify: ask whoever owns the WP install to **purge WP Rocket cache for `/sg/promotions/`**, then retry your test in incognito.

### 2. Browser previous redirect cached
Your Chrome may have cached the 307 from an earlier test where the URL really did redirect (e.g. before a fix, before publishing, while a redirect plugin was active). Browsers aggressively cache 301s; 307s less so but it happens.

How to verify: hard reload + clear site data:
- DevTools → Application → Storage → "Clear site data"
- Or in Network tab tick "Disable cache" before loading

### 3. Client-side JS redirect (not a real HTTP 307)
A JavaScript script could be calling `window.location.replace()` or `history.replaceState()` to clean the URL after page load. In Network tab, this would still show the original 200 request — but if you only looked at the URL and the address bar said no params, you might have logged it as "stripped." **However, a 307 status code in Network tab can ONLY come from a server.** So this isn't the explanation if you genuinely saw 307 in Status column.

### 4. CMP or geo-router plugin firing in your session only
If you're a returning visitor with a specific cookie state (e.g. `cookie_consent=declined`), a privacy plugin might do a server-side redirect to a "consent-stripped" URL.

How to verify: incognito, no cookies, and **check exactly what cookies your browser sent** in the original failing request. Network tab → click the failing request → Headers → "Request Headers" → look for `Cookie:` line.

---

## What to Do Next

### Step 1 — Reproduce cleanly
1. Open incognito (no cookies, no cache, no extensions interfering)
2. DevTools → Network → check "Preserve log" + "Disable cache"
3. Visit `https://extraspaceasia.com/sg/promotions/?utm_source=test&utm_campaign=manualcheck&gclid=ABC123`
4. **If status is 200:** problem doesn't exist; proceed to Step 2
5. **If status is still 307:** click the failing request → Headers tab → copy the **full request headers** (especially `Cookie:`) and the **full response headers**. Send those over.

### Step 2 — Verify Tag 1 actually fires
Once you have a 200 response with params in the URL, run in console:
```javascript
console.log('URL:', window.location.href);
console.log('Search params:', new URL(location.href).searchParams.get('utm_source'));
console.log('Cookie:', document.cookie.split('; ').find(c => c.startsWith('esa_attribution=')));
```

Expected output:
- URL: full URL with params
- Search params: "test"
- Cookie: should start with `esa_attribution=%7B%22utm_source%22...`

If first two pass but cookie is undefined → **Tag 1 isn't firing** → confirm GTM container `GTM-W76SVC9Q` was Submitted/Published, not just Saved.

### Step 3 — Fix the one real issue
The site root `extraspaceasia.com/?utm_source=...` strips params on its 302 to `/sg/`. This is a WordPress-level redirect.

**Options:**
- **Operational fix (preferred):** instruct marketing to never point ads at the bare domain. All paid URLs should land on `/sg/landing-page/?...` directly. Cheapest fix.
- **Code fix:** find the WordPress redirect rule (likely Polylang default-language rule or a custom `wp_redirect()` call) and modify it to preserve `$_SERVER['QUERY_STRING']`.

---

## Bottom Line

The infrastructure is **not** broken in a way that prevents attribution capture on the form. Your 307 is environment-specific and very likely a WP Rocket / browser cache artifact.

The code in `01-capture-attribution.html` and `02-populate-form.html` will work correctly once:
1. You confirm the 307 doesn't reproduce in clean incognito
2. The GTM container with the tags is **published** (not just saved)
3. The Gravity hidden fields exist with matching IDs
