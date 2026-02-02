"""
Main routes - dashboard, landing pages, healthcheck.
"""

import sys
from datetime import datetime
from flask import Blueprint, render_template, redirect, url_for, jsonify, current_app
from flask_login import login_required, current_user

main_bp = Blueprint('main', __name__)


@main_bp.route('/')
def index():
    """Landing page - redirect to dashboard if logged in."""
    if current_user.is_authenticated:
        return redirect(url_for('main.dashboard'))
    return redirect(url_for('auth.login'))


@main_bp.route('/dashboard')
@login_required
def dashboard():
    """Main dashboard page."""
    return render_template('dashboard.html', user=current_user)


@main_bp.route('/healthcheck')
def healthcheck():
    """System health check endpoint."""
    import requests

    # Check database connection
    db_status = 'unknown'
    try:
        from sqlalchemy import text
        session = current_app.get_db_session()
        session.execute(text('SELECT 1'))
        session.close()
        db_status = 'connected'
    except Exception as e:
        db_status = f'error: {str(e)[:50]}'

    # Check scheduler API
    scheduler_status = 'unknown'
    try:
        resp = requests.get('http://127.0.0.1:5000/api/status', timeout=2)
        if resp.status_code == 200:
            data = resp.json()
            scheduler_status = data.get('status', 'unknown')
    except Exception:
        scheduler_status = 'unavailable'

    # Calculate uptime
    uptime_seconds = None
    if hasattr(current_app, 'web_started_at'):
        uptime_seconds = (datetime.now() - current_app.web_started_at).total_seconds()

    return jsonify({
        'status': 'healthy',
        'python_version': sys.version,
        'database': db_status,
        'scheduler': scheduler_status,
        'uptime_seconds': uptime_seconds,
        'timestamp': datetime.utcnow().isoformat()
    })
