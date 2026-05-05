"""Admin UI for unit-category risk factor inspection and override.

Renders /admin/risk-factors which fetches data via /api/risk/* endpoints
and via the inventory JSON endpoint added here in Task 15.
"""
from flask import Blueprint, render_template
from flask_login import login_required

from common.config_loader import get_config
from web.auth.decorators import risk_admin_access_required

bp = Blueprint("admin_risk", __name__, url_prefix="/admin")


@bp.route("/risk-factors", methods=["GET"])
@login_required
@risk_admin_access_required
def risk_factors_page():
    cfg = get_config().get_section("risk").to_dict()
    return render_template(
        "admin/risk_factors.html",
        countries=cfg["countries"],
        bands=cfg["gradient_bands"],
    )
