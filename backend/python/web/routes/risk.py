"""Read + write API for unit-category risk scoring.

GET  /api/risk/unit-category   — single-unit lookup
GET  /api/risk/factors         — full matrix dump for one country
PUT  /api/risk/factors/<id>    — set / clear override (Task 12)
POST /api/risk/recompute       — admin-trigger pipeline run (Task 12)
"""
from __future__ import annotations

import logging

from flask import Blueprint, current_app, jsonify, request
from sqlalchemy import text

from common.config_loader import get_config
from common.risk_lookup import FactorRow, compute_risk, resolve_effective_factor
from web.auth.jwt_auth import require_auth, require_api_scope
from web.utils.rate_limit import rate_limit_api

logger = logging.getLogger(__name__)
bp = Blueprint("risk", __name__, url_prefix="/api/risk")

DIMENSIONS = ("size", "range", "type", "climate", "shape", "pillar")


def _pbi_session():
    """Lazy session — overridden in tests."""
    return current_app.get_pbi_session()


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


@bp.route("/factors/<int:factor_id>", methods=["PUT"])
@require_auth
@require_api_scope("risk:admin")
@rate_limit_api(max_requests=20, window_seconds=60)
def put_factor_override(factor_id: int):
    from web.utils.audit import audit_log, AuditEvent
    from flask_login import current_user

    body = request.get_json(silent=True) or {}
    reason = (body.get("reason") or "").strip()
    if "override_factor" not in body:
        return jsonify({"error": "override_factor is required"}), 400
    raw = body["override_factor"]

    if raw is None:
        new_override = None
    else:
        try:
            new_override = float(raw)
        except (TypeError, ValueError):
            return jsonify({"error": "override_factor must be a number or null"}), 400
        if not (0.1 <= new_override <= 5.0):
            return jsonify({"error": "override_factor must be between 0.1 and 5.0"}), 400
        if not reason:
            return jsonify({"error": "reason is required when setting override"}), 400

    session = _pbi_session()
    try:
        existing = session.execute(text("""
            SELECT country_code, dimension, value, empirical_factor,
                   is_thin_data, override_factor
              FROM unit_category_risk_factor WHERE id = :id
        """), {"id": factor_id}).fetchone()
        if not existing:
            return jsonify({"error": "factor not found"}), 404
        emp = float(existing[3]) if existing[3] is not None else None
        is_thin = bool(existing[4])
        eff, _ = resolve_effective_factor(emp, new_override, is_thin)

        username = getattr(current_user, "username", None) or "system"
        old_override = float(existing[5]) if existing[5] is not None else None

        session.execute(text("""
            UPDATE unit_category_risk_factor
               SET override_factor = :ovr,
                   override_reason = :rsn,
                   override_by = :usr,
                   override_at = CURRENT_TIMESTAMP,
                   effective_factor = :eff
             WHERE id = :id
        """), {"ovr": new_override,
               "rsn": reason if new_override is not None else None,
               "usr": username if new_override is not None else None,
               "eff": eff, "id": factor_id})
        session.commit()

        event = (AuditEvent.RISK_OVERRIDE_SET if new_override is not None
                 else AuditEvent.RISK_OVERRIDE_CLEARED)
        audit_log(
            event,
            f"factor_id={factor_id} country={existing[0]} "
            f"dimension={existing[1]} value={existing[2]} "
            f"old={old_override} new={new_override} reason={reason!r}",
        )
        return jsonify({"status": "success", "data": {
            "id": factor_id, "override_factor": new_override,
            "effective_factor": eff,
        }})
    except Exception:
        logger.exception("override update failed")
        return jsonify({"error": "update failed"}), 500
    finally:
        session.close()


@bp.route("/recompute", methods=["POST"])
@require_auth
@require_api_scope("risk:admin")
@rate_limit_api(max_requests=4, window_seconds=60)
def post_recompute():
    body = request.get_json(silent=True) or {}
    country = body.get("country")
    if country and not isinstance(country, str):
        return jsonify({"error": "country must be a string"}), 400
    try:
        from sync_service.pipelines.unit_category_risk import run as run_pipeline
        summary = run_pipeline(country_code=country.upper() if country else None)
        return jsonify({"status": "success", "data": summary})
    except Exception:
        logger.exception("recompute failed")
        return jsonify({"error": "recompute failed"}), 500
