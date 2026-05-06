"""
Web UI for the Sync Orchestrator (sync_service).

Mounted at /orchestrator/*. Gated by scheduler access permission (same role
that manages scheduler, until a dedicated permission is introduced).

Routes:
    GET  /orchestrator/                       Dashboard — pipeline list + live stats
    GET  /orchestrator/pipelines/<name>       Pipeline detail — freshness, runs, config
    GET  /orchestrator/runs                   Recent runs across all pipelines
"""

from flask import Blueprint, render_template

from web.auth.decorators import login_required, scheduler_access_required

orchestrator_ui_bp = Blueprint(
    'orchestrator_ui',
    __name__,
    url_prefix='/orchestrator',
)


@orchestrator_ui_bp.route('/')
@login_required
@scheduler_access_required
def dashboard():
    """Main orchestrator dashboard."""
    return render_template('orchestrator/dashboard.html')


@orchestrator_ui_bp.route('/pipelines/<name>')
@login_required
@scheduler_access_required
def pipeline_detail(name):
    """Detail view for a single orchestrator pipeline."""
    return render_template('orchestrator/pipeline_detail.html', pipeline_name=name)


@orchestrator_ui_bp.route('/runs')
@login_required
@scheduler_access_required
def runs():
    """Recent runs across all pipelines."""
    return render_template('orchestrator/runs.html')
