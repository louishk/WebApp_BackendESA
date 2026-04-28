"""
AI-assisted extraction of discount plans from a promo-brief paste.

Caller supplies the pasted document (Excel copy-paste = tab-separated text).
The extractor sends it to the shared Azure AI Foundry chat model via the same
OpenAI-compatible client pattern used by `web.utils.translation` and returns a
list of candidate-plan dicts shaped to match the DiscountPlan form fields.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


EXTRACT_SYSTEM_PROMPT = """You are extracting discount / promotion plans from a marketing brief used by Extra Space Asia (a self-storage company).

The brief is typically a matrix:
- ROWS = plan attributes (Name, Marketing, Period Range, Site Location, Offers, Payment, T&Cs, etc.)
- COLUMNS = one plan each, usually grouped by (country × storage type) — e.g. SG Self Storage, SG Wine Storage, MY Self Storage, KR Self Storage.

Every column with real content is ONE discount plan. Skip empty columns.

Return a JSON object with exactly one key "plans" whose value is an array. Each element MUST have these keys (use null when unknown):
- plan_name: short identifier like "Moving Season SG Self Storage 2026-Q2"
- plan_type: one of ["Tactical", "Seasonal", "Evergreen"]
- objective: string, short business objective if stated
- country: ISO-ish code — one of ["SG","MY","KR","HK","JP","TW","TH"]
- storage_type: one of ["Self Storage", "Wine Storage"]
- promo_period_start: "YYYY-MM-DD"
- promo_period_end: "YYYY-MM-DD"
- booking_period_start: "YYYY-MM-DD" or null
- booking_period_end: "YYYY-MM-DD" or null
- discount_type: one of ["percentage","fixed_amount","free_period","none"]
- discount_numeric: number (e.g. 30 for "30% off")
- payment_terms: one of ["Flexible","Prepaid","Fixed"]
- deposit: short string like "No Deposit" or "1 Month"
- lock_in_period: short string like "Min 6M up to 12M" or "None"
- distribution_channel: comma-separated string of channels (e.g. "All Channels" or "Direct Mail, Chatbot")
- hidden_rate: boolean — TRUE when the brief says the rate is hidden / private / unlocked-by-code / for partners only / non-public. Else FALSE.
- coupon_code: string — extract any explicit promo code mentioned (e.g. "Use code MOVING2026", "Code: SAVE50"). Null if no code is mentioned. Required when hidden_rate=TRUE; if you mark hidden_rate but find no code, leave coupon_code null and the admin will fill it in.
- switch_to_us: one of ["Eligible","Not Eligible","Eligible with Conditions"]
- referral_program: one of ["Eligible","Not Eligible","Eligible with Conditions"]
- terms_conditions: array of strings — one numbered T&C clause per element (strip leading numbers)
- tc_labels: array of strings of the SAME length as terms_conditions — the category label for each clause (e.g. "Eligibility","Payment Policy")
- rate_rules: string — text from the "Rate Rules" row (usually under the REV section). Keep the text as-is. Return null if the cell is empty or a dash.
- rate_rules_sites: string — any specific sites mentioned alongside rate_rules, else null.
- sitelink_plan_name: string — value from the "Sitelink Discount Plan" row (usually under the OPS section). This is the SiteLink concession plan name (e.g. "Moving Season Promo"). Null if empty.
- notes: string (any residual text worth keeping)
- group_name: string — a SINGLE short label that groups every plan in this brief together (e.g. "Moving Season 2026-Q2"). Derive it from the overall campaign title / period shown in the brief. Use the SAME group_name on every plan you return — they're siblings of one campaign.

Rules:
- Dates: parse English like "May 1st to July 31th, 2026" → 2026-05-01 and 2026-07-31.
- Be conservative: only populate a field when the document clearly states a value for that column.
- T&C numbering: strip the leading "N." from each clause before placing it in the array.
- Do NOT invent data. If a column has no T&Cs, return an empty array.

Return ONLY the JSON object. No prose. Output COMPACT JSON — no pretty-printing, no extra whitespace between keys/values. This matters: long briefs hit token limits."""


def _get_client():
    from openai import OpenAI
    from common.config_loader import get_config

    foundry = get_config().llm.azure_foundry
    base_url = getattr(foundry, 'base_url', None)
    model = getattr(foundry, 'model', 'grok-3-mini')
    api_key = getattr(foundry, 'api_key_vault', None)
    if not base_url or not api_key:
        raise RuntimeError(
            "Azure AI Foundry not configured — check config/llm.yaml + vault AZURE_FOUNDRY_API_KEY."
        )
    return OpenAI(base_url=base_url, api_key=api_key), model


def extract_plans(document_text: str) -> List[Dict[str, Any]]:
    """Send the pasted promo document to the LLM and return candidate plans.

    Raises RuntimeError if the model isn't configured or the response can't
    be parsed as JSON.
    """
    if not document_text or not document_text.strip():
        return []

    # Try to pre-structure a tab-separated Excel paste into per-column blocks
    # so the LLM gets a clearer signal than a wall of tabs. Falls back to the
    # raw text if the paste doesn't look like a matrix.
    structured = _restructure_matrix_paste(document_text)
    user_content = structured if structured else document_text[:60000]

    client, model = _get_client()
    # Grok-3-mini is a reasoning model. Azure AI Foundry accepts
    # reasoning_effort in {medium, high}; use medium to keep latency lower.
    kwargs = dict(
        model=model,
        messages=[
            {"role": "system", "content": EXTRACT_SYSTEM_PROMPT},
            {"role": "user", "content": user_content[:60000]},
        ],
        max_tokens=16000,
        temperature=0.1,
    )
    try:
        resp = client.chat.completions.create(
            **kwargs, extra_body={'reasoning_effort': 'medium'},
        )
    except TypeError:
        # Older SDKs don't take extra_body kwarg.
        resp = client.chat.completions.create(**kwargs)
    except Exception as e:
        # If Foundry still rejects the reasoning_effort param, retry without it.
        msg = str(e)
        if 'reasoning_effort' in msg or 'Bad Request' in msg:
            logger.warning(
                f"promo_extractor: reasoning_effort rejected ({msg!r}); retrying without it."
            )
            try:
                resp = client.chat.completions.create(**kwargs)
            except Exception as e2:
                logger.error(f"promo_extractor: LLM API call failed: {e2}")
                raise RuntimeError(f"LLM API call failed: {e2}")
        else:
            logger.error(f"promo_extractor: LLM API call failed: {e}")
            raise RuntimeError(f"LLM API call failed: {e}")

    choice = resp.choices[0] if resp.choices else None
    raw = (choice.message.content if choice and choice.message else '') or ''
    finish_reason = getattr(choice, 'finish_reason', None) if choice else None
    logger.info(
        f"promo_extractor: model={model} finish={finish_reason} len={len(raw)} "
        f"usage={getattr(resp, 'usage', None)}"
    )

    json_blob = _extract_json_blob(raw)
    if not json_blob:
        # Sometimes the model runs out of tokens before even closing the root
        # object. Salvage whatever complete plan entries are available.
        if finish_reason == 'length' and raw.lstrip().startswith('{'):
            salvaged = _salvage_partial_plans(raw)
            if salvaged:
                logger.warning(
                    f"promo_extractor: output truncated (finish=length, len={len(raw)}); "
                    f"salvaged {len(salvaged)} plan(s) from the partial JSON."
                )
                return [_normalise(p) for p in salvaged if isinstance(p, dict)]
        logger.error(
            f"promo_extractor: LLM returned non-JSON. finish={finish_reason} "
            f"first_200={raw[:200]!r}"
        )
        if finish_reason == 'length':
            raise RuntimeError(
                "The model hit its output token limit before finishing the JSON. "
                "Try pasting fewer columns at once (e.g. split country tabs into "
                "separate runs)."
            )
        raise RuntimeError(
            "LLM returned a response that didn't contain a JSON object. "
            f"finish_reason={finish_reason}, response_length={len(raw)}."
        )

    try:
        parsed = json.loads(json_blob)
    except json.JSONDecodeError as e:
        logger.error(f"promo_extractor: JSON parse failed: {e}; blob first_300={json_blob[:300]!r}")
        raise RuntimeError(f"LLM returned invalid JSON: {e}")

    plans = parsed.get('plans') if isinstance(parsed, dict) else None
    if not isinstance(plans, list):
        raise RuntimeError("LLM response missing 'plans' array.")

    return [_normalise(p) for p in plans if isinstance(p, dict)]


def _salvage_partial_plans(raw: str) -> List[Dict[str, Any]]:
    """Best-effort extraction of complete plan objects from a truncated blob.

    When the model hits max_tokens mid-array, the trailing object is
    incomplete. We scan for balanced `{...}` objects inside the outer
    `"plans": [ ... ]` and return whichever parsed cleanly.
    """
    try:
        arr_start = raw.find('"plans"')
        if arr_start == -1:
            return []
        bracket = raw.find('[', arr_start)
        if bracket == -1:
            return []
    except Exception:
        return []

    plans: List[Dict[str, Any]] = []
    depth = 0
    in_str = False
    esc = False
    obj_start = None
    for i in range(bracket + 1, len(raw)):
        ch = raw[i]
        if in_str:
            if esc:
                esc = False
            elif ch == '\\':
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
            continue
        if ch == '{':
            if depth == 0:
                obj_start = i
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0 and obj_start is not None:
                blob = raw[obj_start:i + 1]
                try:
                    plans.append(json.loads(blob))
                except json.JSONDecodeError:
                    pass
                obj_start = None
    return plans


def _extract_json_blob(text: str) -> Optional[str]:
    """Pull the first balanced-braces JSON object out of `text`.

    Handles common LLM output quirks: markdown code fences, preambles like
    "Here's the JSON:", trailing prose. Returns the substring from the first
    '{' to the matching '}'. Returns None if no balanced object is found.
    """
    if not text:
        return None
    s = text.strip()

    # Strip ``` fences.
    if s.startswith('```'):
        s = s.lstrip('`')
        # Drop optional language tag (e.g. "json\n...").
        nl = s.find('\n')
        if nl != -1:
            s = s[nl + 1:]
        if s.rstrip().endswith('```'):
            s = s.rstrip()[:-3].rstrip()

    start = s.find('{')
    if start == -1:
        return None

    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(s)):
        ch = s[i]
        if in_str:
            if esc:
                esc = False
            elif ch == '\\':
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
            continue
        if ch == '{':
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0:
                return s[start:i + 1]
    return None


# ---------------------------------------------------------------------------
# Normalisation — coerce loose LLM output into the exact shape the form wants
# ---------------------------------------------------------------------------

_PLAN_TYPES = {'Tactical', 'Seasonal', 'Evergreen'}
_STORAGE_TYPES = {'Self Storage', 'Wine Storage'}
_DISCOUNT_TYPES = {'percentage', 'fixed_amount', 'free_period', 'none'}
_PAYMENT_TERMS = {'Flexible', 'Prepaid', 'Fixed'}
_ELIGIBILITY = {'Eligible', 'Not Eligible', 'Eligible with Conditions'}
_COUNTRY_CODES = {'SG', 'MY', 'KR', 'HK', 'JP', 'TW', 'TH'}


def _pick(value, allowed: set):
    if not isinstance(value, str):
        return None
    v = value.strip()
    return v if v in allowed else None


def _to_date_str(v) -> Optional[str]:
    if v is None or v == '':
        return None
    if isinstance(v, (date, datetime)):
        return v.strftime('%Y-%m-%d')
    s = str(v).strip()
    # Accept already-formatted YYYY-MM-DD.
    try:
        return datetime.strptime(s, '%Y-%m-%d').strftime('%Y-%m-%d')
    except ValueError:
        pass
    # Fallback: try a few common formats.
    for fmt in ('%d/%m/%Y', '%m/%d/%Y', '%d %b %Y', '%d %B %Y', '%Y/%m/%d'):
        try:
            return datetime.strptime(s, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return None


def _to_number(v):
    if v is None or v == '':
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _string_array(v) -> List[str]:
    if not isinstance(v, list):
        return []
    return [str(x).strip() for x in v if x is not None and str(x).strip()]


def _normalise(plan: Dict[str, Any]) -> Dict[str, Any]:
    """Map LLM output into form-ready values with strict enums."""
    terms = _string_array(plan.get('terms_conditions'))
    labels = _string_array(plan.get('tc_labels'))
    # Pad/truncate labels to match terms length.
    if len(labels) < len(terms):
        labels = labels + [''] * (len(terms) - len(labels))
    else:
        labels = labels[:len(terms)]

    country_raw = plan.get('country')
    country = country_raw.strip().upper() if isinstance(country_raw, str) else None
    if country not in _COUNTRY_CODES:
        country = None

    return {
        'plan_name': str(plan.get('plan_name') or '').strip() or None,
        'plan_type': _pick(plan.get('plan_type'), _PLAN_TYPES) or 'Tactical',
        'objective': str(plan.get('objective') or '').strip() or None,
        'country': country,
        'storage_type': _pick(plan.get('storage_type'), _STORAGE_TYPES),
        'promo_period_start': _to_date_str(plan.get('promo_period_start')),
        'promo_period_end': _to_date_str(plan.get('promo_period_end')),
        'booking_period_start': _to_date_str(plan.get('booking_period_start')),
        'booking_period_end': _to_date_str(plan.get('booking_period_end')),
        'discount_type': _pick(plan.get('discount_type'), _DISCOUNT_TYPES),
        'discount_numeric': _to_number(plan.get('discount_numeric')),
        'payment_terms': _pick(plan.get('payment_terms'), _PAYMENT_TERMS),
        'deposit': str(plan.get('deposit') or '').strip() or None,
        'lock_in_period': str(plan.get('lock_in_period') or '').strip() or None,
        'distribution_channel': str(plan.get('distribution_channel') or '').strip() or None,
        'switch_to_us': _pick(plan.get('switch_to_us'), _ELIGIBILITY),
        'referral_program': _pick(plan.get('referral_program'), _ELIGIBILITY),
        'terms_conditions': terms,
        'tc_labels': labels,
        'rate_rules': _clean_text(plan.get('rate_rules')),
        'rate_rules_sites': _clean_text(plan.get('rate_rules_sites')),
        'sitelink_plan_name': _clean_text(plan.get('sitelink_plan_name')),
        'group_name': _clean_text(plan.get('group_name')),
        'notes': str(plan.get('notes') or '').strip() or None,
    }


def _clean_text(v) -> Optional[str]:
    """Return a trimmed string, or None if the value is blank or a placeholder dash."""
    if v is None:
        return None
    s = str(v).strip()
    if not s or s in {'-', '—', '–', 'N/A', 'n/a', 'null', 'None'}:
        return None
    return s


# ---------------------------------------------------------------------------
# Matrix paste restructurer
# ---------------------------------------------------------------------------

_COUNTRY_HINTS = {'SG', 'MY', 'KR', 'HK', 'JP', 'TW', 'TH'}
_STORAGE_HINTS = {'Self Storage', 'Wine Storage'}


def _restructure_matrix_paste(text: str) -> Optional[str]:
    """Convert a tab-separated matrix paste into per-column structured blocks.

    The Excel paste from our promo template has:
      - ~2 header rows carrying country codes (SG/MY/KR/...) and storage type
      - a left "section / label" column (or two)
      - N data columns, one per plan

    This function parses the grid, finds column headers, and emits a cleaner
    document the LLM can digest:

        === COLUMN 1 | SG · Self Storage ===
        Name: Moving Season. Get Extra
        Period Range: May 1st to July 31th, 2026
        Site Location: All SG Sites
        Offers: 30% Off on selected Units
        ...

        === COLUMN 2 | SG · Wine Storage ===
        ...

    Returns None if the paste doesn't look like a tab-separated matrix so the
    caller can fall back to the raw text.
    """
    if not text or '\t' not in text:
        return None

    rows = [r.split('\t') for r in text.splitlines()]
    # Strip trailing empty cells on each row but keep the row length for alignment.
    max_cols = max((len(r) for r in rows), default=0)
    if max_cols < 3 or len(rows) < 4:
        return None

    # Pad all rows to the same width for clean indexing.
    for r in rows:
        if len(r) < max_cols:
            r.extend([''] * (max_cols - len(r)))

    # Identify header rows: those with country hints (SG/MY/KR/...) and storage types.
    country_row_idx = None
    storage_row_idx = None
    for i, r in enumerate(rows[:6]):
        tokens = [c.strip() for c in r]
        if country_row_idx is None and any(t in _COUNTRY_HINTS for t in tokens):
            country_row_idx = i
        if storage_row_idx is None and any(t in _STORAGE_HINTS for t in tokens):
            storage_row_idx = i
    if country_row_idx is None and storage_row_idx is None:
        return None  # doesn't match our template

    # Build per-column (country, storage_type) labels. Country typically spans
    # multiple columns (merged cell in Excel → empty neighbours). Forward-fill.
    def _forward_fill(row_idx):
        out = [''] * max_cols
        if row_idx is None:
            return out
        last = ''
        for i, cell in enumerate(rows[row_idx]):
            v = cell.strip()
            if v:
                last = v
            out[i] = last
        return out

    countries = _forward_fill(country_row_idx)
    storage_types = _forward_fill(storage_row_idx)

    # The leftmost 1–2 columns are section + label (Dept + Items). Detect how
    # many non-empty-cell columns are to the left of where country hints start.
    header_start = None
    for idx, v in enumerate(countries):
        if v in _COUNTRY_HINTS:
            header_start = idx
            break
    if header_start is None:
        # No country hit — assume two label columns (matches the template).
        header_start = 2

    data_col_indexes = [i for i in range(header_start, max_cols)
                        if (countries[i] in _COUNTRY_HINTS)
                        or (storage_types[i] in _STORAGE_HINTS)]
    if not data_col_indexes:
        # Fall back: treat every column >= header_start as a data column.
        data_col_indexes = list(range(header_start, max_cols))

    # Dedup consecutive duplicates while preserving order (e.g., if both SG columns
    # point at the same forward-filled country).
    data_col_indexes = list(dict.fromkeys(data_col_indexes))

    # For each row below the headers, build the label from columns 0..header_start-1.
    header_last_idx = max(country_row_idx or 0, storage_row_idx or 0)
    body_rows = rows[header_last_idx + 1:]

    blocks: List[str] = []
    for col in data_col_indexes:
        lines = [f"=== COLUMN {col} | {countries[col] or '?'} · {storage_types[col] or '?'} ==="]
        for r in body_rows:
            label_parts = [p.strip() for p in r[:header_start] if p.strip()]
            if not label_parts:
                continue
            label = ' / '.join(label_parts)
            val = (r[col] if col < len(r) else '').strip()
            if not val:
                continue
            # Collapse internal newlines/tabs to spaces so each field is one line.
            val_clean = ' '.join(val.split())
            lines.append(f"{label}: {val_clean}")
        if len(lines) > 1:
            blocks.append('\n'.join(lines))

    if not blocks:
        return None

    header_note = (
        "The source was an Excel matrix; it has been transposed into one block "
        "per column. Each COLUMN header names the (country · storage_type) pair. "
        "Treat every block as ONE discount plan candidate.\n\n"
    )
    return header_note + '\n\n'.join(blocks)
