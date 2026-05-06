# Get Started Form — Audit Findings

**Source:** `https://extraspaceasia.com/sg/get-started/`
**Date:** 2026-04-27

## Confirmed

| Item | Value |
|---|---|
| Form plugin | **Gravity Forms** (also Contact Form 7 is loaded site-wide, not used on this page) |
| Form ID | **1** (DOM: `gform_1`, attribute: `data-formid='1'`) |
| Action | `/sg/get-started/` |
| Method | POST, multipart/form-data |
| Render mode | **Server-side rendered** (fields present in initial HTML — no AJAX mount, no race condition risk) |
| GTM container on site | `GTM-W76SVC9Q` |

## Existing fields (form 1)

| Field ID | Type | Label | Visible by default |
|---|---|---|---|
| input_1_1 | select | Storage Facility * | yes |
| input_1_3 | select | Storage Type * | yes |
| input_1_4 | select | Storage Size * | yes |
| input_1_5 | select | Storage Duration * | yes |
| input_1_6 | select | Salutation * | yes |
| input_1_8 | text | Name * | yes |
| input_1_9 | email | Email * | yes |
| input_1_10 | phone | Contact Number * | yes |
| input_1_12 | select | Storage Type * | conditional |
| input_1_13 | select | Storage Type * | conditional |
| input_1_14 | select | Storage Type * | conditional |
| input_1_15 | select | Storage Type * | conditional |
| input_1_16 | select | Storage Size | conditional |
| input_1_17 | text | Select Promotion | conditional |
| input_1_18 | select | Select Promotion for Wine Storage | conditional |
| input_1_19 | select | Select Promotion for Self Storage | conditional |

**Highest field ID in use:** 19 → next new field will be ID 20.

## What's missing (must add)

**No attribution hidden fields exist yet.** None of utm_source, gclid, fbclid, etc. are on the form. All 21 fields from our capture plan need to be added.

The "Select Promotion" fields you saw earlier are conditional `text` and `select` inputs (visibility-toggled by other field values), **not** Gravity's "Hidden" field type.

## Discovery convention (active 2026-04-27)

Each hidden field has its **Default Value** set to the param name (matching the
"Allow dynamic population" parameter name). Tag 2 reads `input.value` on first
render — if it matches a known param key, that's the field's identity. No
hardcoded ID map in JS, no CSS classes needed.

To add a new attribution param later:
1. Add Hidden field in Gravity → Default Value + Param Name = param key
2. Add the param key to Tag 1's `URL_PARAMS` or `COOKIE_FIELDS`
3. Add the param key to Tag 2's `KNOWN_PARAMS` whitelist

No JS field-ID changes ever required.

## Confirmed field IDs (verified via populate test 2026-04-27)

| Field ID | Param name | Source | Status |
|---|---|---|---|
| input_1_20 | utm_source | URL | ✅ |
| input_1_21 | utm_medium | URL | ✅ |
| input_1_22 | utm_campaign | URL | ✅ |
| input_1_23 | utm_term | URL | ✅ |
| input_1_24 | utm_content | URL | ✅ |
| input_1_25 | gclid | URL | ✅ |
| input_1_26 | google_client_id | Cookie (`_ga`) | ✅ |
| input_1_27 | fbclid | URL | ✅ |
| input_1_28 | landing_page | JS | ✅ |
| input_1_29 | referrer | JS | ✅ |
| input_1_30 | msclkid | URL | ✅ |
| input_1_31 | gbraid | URL | ✅ |
| input_1_32 | fbp | Cookie (`_fbp`) | ✅ |
| input_1_33 | ttp | Cookie (`_ttp`) | ✅ |
| input_1_34 | ttclid | URL | ✅ |
| input_1_35 | gcl_dc | Cookie (`_gcl_dc`) | ✅ |
| input_1_36 | li_fat_id | URL/Cookie | ✅ |
| input_1_37 | gcl_aw | Cookie (`_gcl_aw`) | ✅ |
| input_1_38 | dclid | URL | ✅ |

## Not yet added (optional)

- `wbraid` — Google iOS web-only click ID. Add only if iOS Google Ads attribution matters.
- `uetmsclkid` — Microsoft UET cookie. Overlaps with `msclkid` URL param; only adds value if cross-session MS Ads attribution matters.

The "Allow dynamic population" param column gives you the free URL-param fallback: if a user lands on `/sg/get-started/?utm_source=google`, Gravity itself will populate `input_1_20` server-side before our JS even runs.

## How to verify field IDs after adding

After saving the form, load the page in a browser, view source (Ctrl+U), and grep for `name="input_1_`. You'll see the actual assigned IDs — paste them in here and we'll update `FIELD_MAP` to match.

Or in DevTools console on the page:
```javascript
[...document.querySelectorAll('input[type=hidden][name^="input_1_"]')]
  .forEach(el => console.log(el.name, el.id))
```

## Already correct in our code
- `02-populate-form.html` uses form ID 1 (matches `gform_1`)
- `gform_post_render` handler still useful for post-validation re-render (form does AJAX-validate even though initial render is server-side)
- `DOMContentLoaded` works fine since fields are present at initial render

## Notes / things to know
- The form **uses gform AJAX submission** (not full page POST). After submit, Gravity replaces the form with a confirmation message in-place — `gform_post_render` may fire then with the form gone. Our populate function handles this safely (no fields → no writes).
- Form has **5 "Storage Type" fields** (input_1_3, 12, 13, 14, 15) — these are conditional variants per facility. Worth confirming with whoever maintains the form that the conditional logic still routes correctly.
- The form hits `/sg/get-started/` as its action — Gravity uses an AJAX hook to intercept this. Webhook-to-Sugar fires server-side after Gravity processes the submission.
