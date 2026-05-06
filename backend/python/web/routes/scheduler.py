"""
Scheduler routes - dashboard pages for job management.

Note: legacy sync orchestrator routes (`/sync`, `/dead-letters`, `/api/sync/*`)
were removed when the orphan `sync/` module was decommissioned. The new
orchestrator UI lives under `/orchestrator/` (orchestrator_ui_bp) and its API
under `/api/orchestrator/` (sync_service_bp).
"""

import logging
from flask import Blueprint, render_template, send_from_directory
from flask_login import login_required
from pathlib import Path

from web.auth.decorators import scheduler_access_required

logger = logging.getLogger(__name__)

scheduler_bp = Blueprint('scheduler', __name__, url_prefix='/scheduler')


@scheduler_bp.route('/')
@login_required
@scheduler_access_required
def dashboard():
    """Main scheduler dashboard page."""
    return render_template('scheduler/dashboard.html')


@scheduler_bp.route('/jobs')
@login_required
@scheduler_access_required
def jobs_page():
    """Jobs management page."""
    return render_template('scheduler/jobs.html')


@scheduler_bp.route('/history')
@login_required
@scheduler_access_required
def history_page():
    """Execution history page."""
    return render_template('scheduler/history.html')


@scheduler_bp.route('/settings')
@login_required
@scheduler_access_required
def settings_page():
    """Settings and administration page."""
    return render_template('scheduler/settings.html')


@scheduler_bp.route('/static/logo.jpeg')
def serve_logo():
    """Serve the logo from static folder."""
    static_path = Path(__file__).parent.parent / 'static' / 'img'
    return send_from_directory(static_path, 'logo.jpeg')
