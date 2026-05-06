"""Risk Factors UI — unit-category and (future) tenant risk inspection."""
from flask import Blueprint, render_template
from flask_login import login_required

from common.config_loader import get_config
from web.auth.decorators import risk_admin_access_required

bp = Blueprint("admin_risk", __name__, url_prefix="")


@bp.route("/risk-factors", methods=["GET"])
@login_required
@risk_admin_access_required
def risk_factors_page():
    """Unit-category risk factors page (the active one)."""
    cfg = get_config().get_section("risk").to_dict()
    return render_template(
        "admin/risk_factors.html",
        countries=cfg["countries"],
        bands=cfg["gradient_bands"],
    )


@bp.route("/risk-factors/tenant", methods=["GET"])
@login_required
@risk_admin_access_required
def tenant_risk_page():
    """Tenant risk factors — placeholder until the model is designed."""
    return render_template("admin/risk_factors_tenant.html")


@bp.route("/risk-factors/inventory", methods=["GET"])
@login_required
@risk_admin_access_required
def risk_inventory_json():
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker
    from flask import request, jsonify

    from common.config_loader import get_database_url
    from common.risk_lookup import (
        FactorRow, compute_risk, resolve_effective_factor,
    )
    from datalayer.unit_category_risk import _explode_dims

    country_code = (request.args.get("country") or "").strip().upper()
    cfg = get_config().get_section("risk").to_dict()
    name = cfg["countries"].get(country_code)
    if not name:
        return jsonify({"error": "country not configured"}), 404

    engine = create_engine(get_database_url("pbi"))
    session = sessionmaker(bind=engine)()
    try:
        baseline_row = session.execute(text("""
            SELECT baseline_rate FROM unit_category_risk_baseline
             WHERE country_code = :cc"""), {"cc": country_code}).fetchone()
        if not baseline_row:
            return jsonify({"error": "baseline missing"}), 404
        baseline_rate = float(baseline_row[0])

        factor_rows = session.execute(text("""
            SELECT dimension, value, empirical_factor, override_factor,
                   is_thin_data, sample_size
              FROM unit_category_risk_factor
             WHERE country_code = :cc"""), {"cc": country_code}).fetchall()
        cache = {}
        for r in factor_rows:
            emp = float(r[2]) if r[2] is not None else None
            ovr = float(r[3]) if r[3] is not None else None
            eff, src = resolve_effective_factor(emp, ovr, bool(r[4]))
            cache[(r[0], r[1])] = FactorRow(value=r[1], effective=eff,
                                            source=src, is_thin=bool(r[4]),
                                            sample_size=int(r[5]))

        units = session.execute(text("""
            SELECT DISTINCT r."SiteID", r."UnitID", r."sUnit", r."sTypeName", s."SiteCode"
              FROM rentroll r
              JOIN siteinfo s ON s."SiteID" = r."SiteID"
             WHERE s."Country" = :country
               AND r.extract_date = (SELECT MAX(extract_date) FROM rentroll r2
                                      WHERE r2."SiteID" = r."SiteID")
          ORDER BY s."SiteCode", r."sUnit"
             LIMIT 1000
        """), {"country": name}).fetchall()

        bands = cfg["gradient_bands"]
        out = []
        for site_id, unit_id, s_unit, s_type, site_code in units:
            dims = _explode_dims(s_type or "")
            if not dims:
                out.append({"site_code": site_code, "unit": s_unit,
                            "s_type": s_type, "parsed_ok": False})
                continue
            factors = {}
            for dim, val in dims.items():
                if not val:
                    continue
                factors[dim] = cache.get((dim, val), FactorRow(
                    value=val, effective=1.0, source="missing",
                    is_thin=True, sample_size=0))
            result = compute_risk(baseline_rate, factors, bands)
            out.append({
                "site_code": site_code, "unit": s_unit, "s_type": s_type,
                "parsed_ok": True,
                "dims": dims,
                "risk_pct": round(result.risk_pct * 100, 3),
                "composite": round(result.composite_factor, 3),
                "delta_pct": round(result.delta_vs_baseline_pct, 1),
                "band": result.band["label"],
                "color": result.band["color"],
                "factor_breakdown": [
                    {"dim": d, "value": f.value, "factor": round(f.effective, 3),
                     "is_thin": f.is_thin}
                    for d, f in factors.items()
                ],
            })
        return jsonify({"status": "success", "data": {
            "baseline_rate_pct": round(baseline_rate * 100, 3),
            "units": out,
        }})
    finally:
        session.close()
