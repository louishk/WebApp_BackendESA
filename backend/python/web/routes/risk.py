"""Read + write API for unit-category risk scoring.

GET  /api/risk/unit-category   — single-unit lookup
GET  /api/risk/factors         — full matrix dump for one country
PUT  /api/risk/factors/<id>    — set / clear override (Task 12)
POST /api/risk/recompute       — admin-trigger pipeline run (Task 12)
"""
from __future__ import annotations

import logging

from flask import Blueprint, jsonify, request
from sqlalchemy import text

from common.config_loader import get_config, get_database_url
from common.risk_lookup import FactorRow, compute_risk, resolve_effective_factor
from web.auth.jwt_auth import require_auth, require_api_scope
from web.utils.rate_limit import rate_limit_api

logger = logging.getLogger(__name__)
bp = Blueprint("risk", __name__, url_prefix="/api/risk")

DIMENSIONS = ("size", "range", "type", "climate", "shape", "pillar")


def _pbi_session():
    """Lazy session — overridden in tests."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    engine = create_engine(get_database_url("pbi"))
    return sessionmaker(bind=engine)()


def _bands():
    return get_config().get_section("risk").to_dict()["gradient_bands"]


def _load_factor_row(session, country_code: str, dim: str, value: str) -> FactorRow:
    row = session.execute(text("""
        SELECT empirical_factor, override_factor, is_thin_data,
               sample_size, effective_factor
          FROM unit_category_risk_factor
         WHERE country_code=:cc AND dimension=:d AND value=:v
    """), {"cc": country_code, "d": dim, "v": value}).fetchone()
    if not row:
        return FactorRow(value=value, effective=1.0, source="missing",
                         is_thin=True, sample_size=0)
    emp = float(row[0]) if row[0] is not None else None
    ovr = float(row[1]) if row[1] is not None else None
    eff, src = resolve_effective_factor(emp, ovr, bool(row[2]))
    return FactorRow(value=value, effective=eff, source=src,
                     is_thin=bool(row[2]), sample_size=int(row[3]))


def _load_baseline(session, country_code: str):
    row = session.execute(text("""
        SELECT baseline_rate, computed_at FROM unit_category_risk_baseline
         WHERE country_code = :cc
    """), {"cc": country_code}).fetchone()
    if not row:
        return None, None
    return float(row[0]), row[1]


@bp.route("/unit-category", methods=["GET"])
@require_auth
@require_api_scope("risk:read")
@rate_limit_api(max_requests=120, window_seconds=60)
def get_unit_category():
    country = (request.args.get("country") or "").strip().upper()
    if not country:
        return jsonify({"error": "country is required"}), 400

    session = _pbi_session()
    try:
        baseline_rate, computed_at = _load_baseline(session, country)
        if baseline_rate is None:
            return jsonify({"error": "country not configured"}), 404

        factors: dict[str, FactorRow] = {}
        for dim in DIMENSIONS:
            value = (request.args.get(dim) or "").strip()
            if not value:
                continue
            factors[dim] = _load_factor_row(session, country, dim, value)

        result = compute_risk(baseline_rate, factors, _bands())
        return jsonify({"status": "success", "data": {
            "country": country,
            "baseline_rate": result.baseline_rate,
            "composite_factor": round(result.composite_factor, 6),
            "risk_pct": round(result.risk_pct, 6),
            "delta_vs_baseline_pct": round(result.delta_vs_baseline_pct, 2),
            "band": result.band["label"],
            "band_color": result.band["color"],
            "factors": {
                dim: {"value": f.value, "factor": round(f.effective, 4),
                      "source": f.source, "is_thin": f.is_thin,
                      "sample_size": f.sample_size}
                for dim, f in factors.items()
            },
            "computed_at": computed_at.isoformat() if computed_at else None,
        }})
    except Exception:
        logger.exception("risk lookup failed")
        return jsonify({"error": "lookup failed"}), 500
    finally:
        session.close()


@bp.route("/factors", methods=["GET"])
@require_auth
@require_api_scope("risk:read")
@rate_limit_api(max_requests=60, window_seconds=60)
def get_factors():
    country = (request.args.get("country") or "").strip().upper()
    if not country:
        return jsonify({"error": "country is required"}), 400
    session = _pbi_session()
    try:
        baseline_rate, computed_at = _load_baseline(session, country)
        if baseline_rate is None:
            return jsonify({"error": "country not configured"}), 404
        rows = session.execute(text("""
            SELECT id, dimension, value, sample_size, empirical_factor,
                   override_factor, effective_factor, is_thin_data,
                   override_reason, override_by, override_at
              FROM unit_category_risk_factor
             WHERE country_code = :cc
          ORDER BY dimension, value
        """), {"cc": country}).fetchall()
        return jsonify({"status": "success", "data": {
            "country": country,
            "baseline_rate": baseline_rate,
            "computed_at": computed_at.isoformat() if computed_at else None,
            "factors": [
                {"id": r[0], "dimension": r[1], "value": r[2],
                 "sample_size": r[3],
                 "empirical_factor": float(r[4]) if r[4] is not None else None,
                 "override_factor": float(r[5]) if r[5] is not None else None,
                 "effective_factor": float(r[6]) if r[6] is not None else 1.0,
                 "is_thin_data": bool(r[7]),
                 "override_reason": r[8], "override_by": r[9],
                 "override_at": r[10].isoformat() if r[10] else None}
                for r in rows
            ],
        }})
    except Exception:
        logger.exception("factor dump failed")
        return jsonify({"error": "lookup failed"}), 500
    finally:
        session.close()
