"""
Marketing feed builder — produces Facebook Catalog and Google Ads custom
remarketing feeds from the recommendation engine's candidate pool.

Grouping: one row per (site_id, stype_name). Within each group we keep the
candidate row with the lowest effective sale price, so the published feed
surfaces the best offer per storage category per site.

Output formats:
  - facebook   → Facebook Product Catalog XML (RSS 2.0 with `g:` namespace)
  - google_ads → Google Ads dynamic remarketing custom feed XML
"""

import logging
import secrets
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Iterable, Optional
from xml.sax.saxutils import escape as xml_escape

from sqlalchemy import text

logger = logging.getLogger(__name__)


ALLOWED_PRICE_COLUMNS = {
    "std_rate", "web_rate", "push_rate", "board_rate", "preferred_rate",
}

# Country → ISO 4217 currency, used when the feed has no currency_override.
COUNTRY_CURRENCY = {
    "Korea": "KRW", "South Korea": "KRW",
    "Singapore": "SGD",
    "Malaysia": "MYR",
    "Hong Kong": "HKD",
    "Japan": "JPY",
    "Taiwan": "TWD",
}


@dataclass
class FeedConfig:
    id: int
    name: str
    slug: str
    channel: str
    enabled: bool
    site_ids: list
    countries: list
    category_includes: list
    category_excludes: list
    unit_type_excludes: list
    list_price_source: str
    sale_price_source: str
    include_sale_price: bool
    currency_override: Optional[str]
    title_template: str
    description_template: str
    brand: str
    landing_url_template: str
    image_url_template: Optional[str]
    public_token: str


@dataclass
class FeedRow:
    site_id: int
    site_code: str
    site_name: str
    country: str
    stype_name: str
    size_category: Optional[str]
    size_range: Optional[str]
    climate_type: Optional[str]
    unit_type: Optional[str]
    list_price: Optional[Decimal]
    sale_price: Optional[Decimal]
    currency: str
    extras: dict = field(default_factory=dict)


CLIMATE_LABELS = {
    "A":  "Air-Conditioned",
    "NC": "Non Climate-Controlled",
    "RF": "Refrigerated",
}


def _climate_label(code: Optional[str]) -> str:
    if not code:
        return ""
    return CLIMATE_LABELS.get(code.upper(), code)


def _validate_price_column(col: str) -> str:
    if col not in ALLOWED_PRICE_COLUMNS:
        raise ValueError(f"Invalid price column: {col}")
    return col


def generate_public_token() -> str:
    return secrets.token_urlsafe(32)


def load_feed_config(session, feed_id: int) -> Optional[FeedConfig]:
    row = session.execute(text("""
        SELECT id, name, slug, channel, enabled, site_ids, countries,
               category_includes, category_excludes, unit_type_excludes,
               list_price_source, sale_price_source, include_sale_price,
               currency_override, title_template, description_template,
               brand, landing_url_template, image_url_template, public_token
        FROM mw_marketing_feeds
        WHERE id = :id
    """), {"id": feed_id}).fetchone()
    return _row_to_config(row) if row else None


def load_feed_by_slug_token(session, slug: str, token: str) -> Optional[FeedConfig]:
    import hmac
    row = session.execute(text("""
        SELECT id, name, slug, channel, enabled, site_ids, countries,
               category_includes, category_excludes, unit_type_excludes,
               list_price_source, sale_price_source, include_sale_price,
               currency_override, title_template, description_template,
               brand, landing_url_template, image_url_template, public_token
        FROM mw_marketing_feeds
        WHERE slug = :slug AND enabled = TRUE
    """), {"slug": slug}).fetchone()
    if not row:
        return None
    if not hmac.compare_digest(row.public_token or '', token or ''):
        return None
    return _row_to_config(row)


def _row_to_config(row) -> FeedConfig:
    return FeedConfig(
        id=row.id, name=row.name, slug=row.slug, channel=row.channel,
        enabled=row.enabled,
        site_ids=list(row.site_ids or []),
        countries=list(row.countries or []),
        category_includes=list(row.category_includes or []),
        category_excludes=list(row.category_excludes or []),
        unit_type_excludes=list(row.unit_type_excludes or []),
        list_price_source=row.list_price_source,
        sale_price_source=row.sale_price_source,
        include_sale_price=row.include_sale_price,
        currency_override=row.currency_override,
        title_template=row.title_template,
        description_template=row.description_template,
        brand=row.brand,
        landing_url_template=row.landing_url_template,
        image_url_template=row.image_url_template,
        public_token=row.public_token,
    )


def build_rows(session, cfg: FeedConfig) -> list:
    """Run the grouping query and return one FeedRow per (site, category)."""
    list_col = _validate_price_column(cfg.list_price_source)
    sale_col = _validate_price_column(cfg.sale_price_source)

    sql = f"""
        WITH best AS (
            SELECT DISTINCT ON (c.site_id, c.stype_name)
                c.site_id,
                c.site_code,
                c.stype_name,
                c.size_category,
                c.size_range,
                c.climate_type,
                c.unit_type,
                c.{list_col} AS list_price,
                c.{sale_col} AS sale_price
            FROM mw_unit_discount_candidates c
            WHERE c.parse_ok = TRUE
              AND c.stype_name IS NOT NULL
              AND c.{sale_col} IS NOT NULL
              AND (:site_ids_empty OR c.site_id = ANY(:site_ids))
              AND (:unit_excl_empty OR c.unit_type <> ALL(:unit_type_excludes))
              AND (:cat_inc_empty OR c.stype_name ILIKE ANY(:cat_inc_patterns))
              AND (:cat_exc_empty OR NOT (c.stype_name ILIKE ANY(:cat_exc_patterns)))
            ORDER BY c.site_id, c.stype_name, c.{sale_col} ASC NULLS LAST
        )
        SELECT b.*, s."Name" AS site_name, s."Country" AS country
        FROM best b
        JOIN mw_siteinfo s ON s."SiteID" = b.site_id
        WHERE (:countries_empty OR s."Country" = ANY(:countries))
        ORDER BY s."Country", b.site_code, b.stype_name
    """

    params = {
        "site_ids": cfg.site_ids,
        "site_ids_empty": len(cfg.site_ids) == 0,
        "countries": cfg.countries,
        "countries_empty": len(cfg.countries) == 0,
        "unit_type_excludes": cfg.unit_type_excludes,
        "unit_excl_empty": len(cfg.unit_type_excludes) == 0,
        "cat_inc_patterns": [f"%{p}%" for p in cfg.category_includes],
        "cat_inc_empty": len(cfg.category_includes) == 0,
        "cat_exc_patterns": [f"%{p}%" for p in cfg.category_excludes],
        "cat_exc_empty": len(cfg.category_excludes) == 0,
    }

    raw = session.execute(text(sql), params).fetchall()

    rows = []
    for r in raw:
        currency = cfg.currency_override or COUNTRY_CURRENCY.get(r.country, "USD")
        rows.append(FeedRow(
            site_id=r.site_id,
            site_code=r.site_code,
            site_name=r.site_name,
            country=r.country,
            stype_name=r.stype_name,
            size_category=r.size_category,
            size_range=r.size_range,
            climate_type=r.climate_type,
            unit_type=r.unit_type,
            list_price=r.list_price,
            sale_price=r.sale_price,
            currency=currency,
        ))
    return rows


# ---------------------------------------------------------------------------
# Template rendering
# ---------------------------------------------------------------------------

def _render_template(tpl: str, row: FeedRow, cfg: FeedConfig) -> str:
    if not tpl:
        return ""
    ctx = {
        "site_id": row.site_id,
        "site_code": row.site_code,
        "site_name": row.site_name,
        "country": row.country,
        "stype_name": row.stype_name,
        "size_category": row.size_category or "",
        "size_range": row.size_range or "",
        "climate_type": row.climate_type or "",
        "climate_label": _climate_label(row.climate_type),
        "unit_type": row.unit_type or "",
        "brand": cfg.brand,
    }
    try:
        return tpl.format(**ctx)
    except (KeyError, IndexError, AttributeError, TypeError, ValueError) as exc:
        logger.warning("Template render error (tpl=%r): %s", tpl, exc)
        return tpl


def _row_id(row: FeedRow) -> str:
    safe_stype = "".join(c if c.isalnum() else "-" for c in row.stype_name)[:50]
    return f"{row.site_code}-{safe_stype}".lower()


def _format_price(amount: Decimal, currency: str) -> str:
    # JPY/KRW have no decimals.
    if currency in ("JPY", "KRW"):
        return f"{int(amount):d} {currency}"
    return f"{Decimal(amount):.2f} {currency}"


# ---------------------------------------------------------------------------
# Facebook Catalog XML (RSS 2.0 + g: namespace)
# ---------------------------------------------------------------------------

def render_facebook_xml(cfg: FeedConfig, rows: Iterable[FeedRow]) -> str:
    out = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<rss xmlns:g="http://base.google.com/ns/1.0" version="2.0">',
        '<channel>',
        f'<title>{xml_escape(cfg.name)}</title>',
        f'<link>https://www.extraspaceasia.com</link>',
        f'<description>{xml_escape(cfg.name)} — generated {datetime.utcnow().isoformat()}Z</description>',
    ]
    for row in rows:
        if row.sale_price is None:
            continue
        title = _render_template(cfg.title_template, row, cfg)
        desc = _render_template(cfg.description_template, row, cfg)
        link = _render_template(cfg.landing_url_template, row, cfg)
        image = _render_template(cfg.image_url_template or "", row, cfg) or ""

        list_p = row.list_price if row.list_price else row.sale_price
        out.append('<item>')
        out.append(f'<g:id>{xml_escape(_row_id(row))}</g:id>')
        out.append(f'<g:title>{xml_escape(title)}</g:title>')
        out.append(f'<g:description>{xml_escape(desc)}</g:description>')
        out.append(f'<g:link>{xml_escape(link)}</g:link>')
        if image:
            out.append(f'<g:image_link>{xml_escape(image)}</g:image_link>')
        out.append(f'<g:availability>in stock</g:availability>')
        out.append(f'<g:condition>new</g:condition>')
        out.append(f'<g:brand>{xml_escape(cfg.brand)}</g:brand>')
        out.append(f'<g:price>{_format_price(list_p, row.currency)}</g:price>')
        if cfg.include_sale_price and row.list_price and row.sale_price < row.list_price:
            out.append(f'<g:sale_price>{_format_price(row.sale_price, row.currency)}</g:sale_price>')
        out.append(f'<g:google_product_category>Business &amp; Industrial &gt; Storage</g:google_product_category>')
        out.append(f'<g:custom_label_0>{xml_escape(row.site_code)}</g:custom_label_0>')
        out.append(f'<g:custom_label_1>{xml_escape(row.size_category or "")}</g:custom_label_1>')
        out.append(f'<g:custom_label_2>{xml_escape(row.climate_type or "")}</g:custom_label_2>')
        out.append(f'<g:custom_label_3>{xml_escape(row.country)}</g:custom_label_3>')
        out.append('</item>')
    out.append('</channel>')
    out.append('</rss>')
    return "\n".join(out)


# ---------------------------------------------------------------------------
# Google Ads custom remarketing XML
# ---------------------------------------------------------------------------

def render_google_ads_xml(cfg: FeedConfig, rows: Iterable[FeedRow]) -> str:
    out = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<listings>',
        f'<!-- {xml_escape(cfg.name)} — generated {datetime.utcnow().isoformat()}Z -->',
    ]
    for row in rows:
        if row.sale_price is None:
            continue
        title = _render_template(cfg.title_template, row, cfg)
        desc = _render_template(cfg.description_template, row, cfg)
        link = _render_template(cfg.landing_url_template, row, cfg)
        image = _render_template(cfg.image_url_template or "", row, cfg) or ""

        has_discount = (
            cfg.include_sale_price
            and row.list_price
            and row.sale_price < row.list_price
        )
        headline_price = row.list_price if has_discount else row.sale_price

        out.append('<listing>')
        out.append(f'<id>{xml_escape(_row_id(row))}</id>')
        out.append(f'<title>{xml_escape(title)}</title>')
        out.append(f'<description>{xml_escape(desc)}</description>')
        out.append(f'<link>{xml_escape(link)}</link>')
        if image:
            out.append(f'<image_link>{xml_escape(image)}</image_link>')
        out.append(f'<price>{_format_price(headline_price, row.currency)}</price>')
        if has_discount:
            out.append(f'<sale_price>{_format_price(row.sale_price, row.currency)}</sale_price>')
        out.append(f'<contextual_keywords>{xml_escape(row.stype_name)}, {xml_escape(row.site_code)}, storage</contextual_keywords>')
        out.append(f'<site_code>{xml_escape(row.site_code)}</site_code>')
        out.append(f'<category>{xml_escape(row.stype_name)}</category>')
        out.append(f'<country>{xml_escape(row.country)}</country>')
        out.append('</listing>')
    out.append('</listings>')
    return "\n".join(out)


def render_feed(cfg: FeedConfig, rows: list) -> str:
    if cfg.channel == "facebook":
        return render_facebook_xml(cfg, rows)
    if cfg.channel == "google_ads":
        return render_google_ads_xml(cfg, rows)
    raise ValueError(f"Unknown channel: {cfg.channel}")


def mark_feed_built(session, feed_id: int, row_count: int) -> None:
    session.execute(text("""
        UPDATE mw_marketing_feeds
        SET last_built_at = NOW(), last_row_count = :n
        WHERE id = :id
    """), {"id": feed_id, "n": row_count})
    session.commit()
