"""
Main routes - dashboard, landing pages, healthcheck.
"""

import sys
from datetime import datetime
from flask import Blueprint, render_template, redirect, url_for, jsonify, current_app, Response, abort
from flask_login import login_required, current_user

main_bp = Blueprint('main', __name__)


# Content type mapping for page extensions
CONTENT_TYPES = {
    'html': 'text/html; charset=utf-8',
    'css': 'text/css; charset=utf-8',
    'js': 'application/javascript; charset=utf-8',
    'json': 'application/json; charset=utf-8',
    'xml': 'application/xml; charset=utf-8',
    'txt': 'text/plain; charset=utf-8',
}


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
        current_app.logger.error(f"Database health check failed: {e}")
        db_status = 'error'

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


@main_bp.route('/pages')
@login_required
def pages_list():
    """List all pages the current user can view."""
    from web.models import Page

    session = current_app.get_db_session()
    try:
        all_pages = session.query(Page).order_by(Page.title).all()
        # Filter to only pages the user can view
        viewable_pages = [p for p in all_pages if p.can_view(current_user)]
        return render_template('pages/list.html', pages=viewable_pages)
    finally:
        session.close()


@main_bp.route('/<slug>.<extension>')
def serve_page(slug, extension):
    """
    Serve dynamic pages stored in database.
    URLs like /ops.html, /report.json, /styles.css
    """
    from web.models import Page

    session = current_app.get_db_session()
    try:
        page = session.query(Page).filter_by(slug=slug, extension=extension).first()

        if not page:
            abort(404)

        # Check if user can view the page
        if not page.can_view(current_user):
            if not current_user.is_authenticated:
                return redirect(url_for('auth.login'))
            abort(403)

        content_type = CONTENT_TYPES.get(extension, 'text/plain; charset=utf-8')
        return Response(page.content or '', mimetype=content_type)
    finally:
        session.close()
