"""
Statistics routes - API consumption monitoring dashboard.
"""

from flask import Blueprint, render_template
from flask_login import login_required

from web.auth.decorators import scheduler_access_required

statistics_bp = Blueprint('statistics', __name__, url_prefix='/statistics')


@statistics_bp.route('/')
@login_required
@scheduler_access_required
def dashboard():
    """API statistics dashboard page."""
    return render_template('statistics/dashboard.html')
