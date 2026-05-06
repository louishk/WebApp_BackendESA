# ESA Web Tracking

Working folder for the Get Started form attribution pipeline.

**Goal**: enrich SugarCRM lead webhook from `extraspaceasia.com/sg/get-started/` (Gravity Forms, form ID 1) with UTM params, gclid, and landing context — without touching the Gravity → Sugar webhook itself.

**GTM container:** `GTM-W76SVC9Q`

See:
- [diagnostic-report.md](diagnostic-report.md) — server-side investigation of the 307 redirect issue
- [form-audit.md](form-audit.md) — Gravity form structure, existing fields, predicted IDs for new fields

## Approach
Capture attribution into a first-party cookie site-wide, then populate hidden Gravity fields on the form page. Gravity's existing webhook carries the new fields to Sugar.

## Files
- `01-capture-attribution.html` — GTM Custom HTML, fires on All Pages
- `02-populate-form.html` — GTM Custom HTML, fires on `/sg/get-started/` only

## Hidden fields to add in Gravity

UTMs:
- utm_source, utm_medium, utm_campaign, utm_term, utm_content

URL click IDs:
- gclid (Google Ads), fbclid (Meta), msclkid (Microsoft Ads)
- gbraid, wbraid (Google iOS attribution)
- dclid (DV360), ttclid (TikTok)

Cookie-derived (cross-session attribution):
- google_client_id — from `_ga`, joins to GA4 / Ads offline conversions
- fbp — from `_fbp`, primary match key for Meta CAPI
- gcl_aw — from `_gcl_aw`, durable Google Ads click cookie
- gcl_dc — from `_gcl_dc`, DV360 click cookie
- uetmsclkid — from `_uetmsclkid`, Microsoft UET cookie
- ttp — from `_ttp`, TikTok Pixel ID
- li_fat_id — from `li_fat_id`, LinkedIn first-party click ID

Context:
- landing_page, referrer

For each, tick "Allow field to be populated dynamically" and set the param name to match the cookie key (e.g. `utm_source`). Gives free URL-param fallback for direct landings.

## Open items
- [ ] Add the hidden fields above in Gravity form editor; record field IDs
- [ ] Update `FIELD_MAP` in `02-populate-form.html` with real IDs
- [ ] Create matching `*_c` custom fields in Sugar Studio (Leads module)
- [ ] Map new Gravity fields to Sugar fields in the webhook config
- [ ] Decide first-touch vs last-touch with marketing/agency
- [ ] Confirm consent gating with whoever owns the CMP
