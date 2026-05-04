"""
API Key management routes — user-facing.
Each user can have one API key. Scopes are managed by admins under User Management.
Users can generate, view, and regenerate their key here.
"""

from pathlib import Path

from flask import Blueprint, render_template, redirect, url_for, request, flash, current_app, make_response, abort, jsonify
from flask_login import login_required, current_user

from web.utils.audit import audit_log, AuditEvent

api_keys_bp = Blueprint('api_keys', __name__, url_prefix='/api-keys')

# Path to the public integration guide (markdown). Repo-relative so it works
# in dev and on the VM without env-specific config.
_DOCS_API_DIR = Path(__file__).resolve().parents[4] / 'docs' / 'api'
_INTEGRATION_GUIDE_MD   = _DOCS_API_DIR / 'recommendation_engine_public.md'
_INTEGRATION_GUIDE_HTML = _DOCS_API_DIR / 'recommendation_engine_public.html'
_INTEGRATION_GUIDE_PDF  = _DOCS_API_DIR / 'recommendation_engine_public.pdf'


def _is_doc_request_authorised() -> bool:
    """
    Doc access — accept either:
      1. Logged-in session (current_user.is_authenticated)
      2. Valid X-API-Key HEADER

    Header-only on purpose: a credential-in-URL via ?key= would leak to
    nginx access logs and gunicorn stdout. The shareable artifact is the
    downloaded PDF / HTML, not the live URL.

    No scope check, no quota counting (doc fetches must not eat API quota).
    """
    from flask_login import current_user
    if current_user and current_user.is_authenticated:
        return True

    raw = (request.headers.get('X-API-Key') or '').strip()
    if not raw or not raw.startswith('esa_') or '.' not in raw:
        return False
    try:
        key_id, raw_secret = raw[4:].split('.', 1)
    except ValueError:
        return False
    if not key_id or not raw_secret:
        return False

    try:
        from web.models.api_key import ApiKey
        db = current_app.get_db_session()
        try:
            api_key = db.query(ApiKey).filter_by(key_id=key_id).first()
            if not api_key or not api_key.is_valid():
                return False
            if not api_key.verify_secret(raw_secret):
                return False
            return True
        finally:
            db.close()
    except Exception:
        return False


def get_session():
    return current_app.get_db_session()


@api_keys_bp.route('/')
@login_required
def my_key():
    """Show the current user's single API key (or prompt to generate one)."""
    from web.models.api_key import ApiKey, API_SCOPES

    db = get_session()
    try:
        api_key = db.query(ApiKey).filter_by(user_id=current_user.id).first()
        return render_template('api_keys/my_key.html',
                               api_key=api_key,
                               all_scopes=API_SCOPES)
    finally:
        db.close()


@api_keys_bp.route('/generate', methods=['POST'])
@login_required
def generate_key():
    """Generate a new API key for the current user (one per user)."""
    from web.models.api_key import ApiKey, API_SCOPES, generate_api_key, hash_api_secret

    db = get_session()
    try:
        # Check if the user already has a key
        existing = db.query(ApiKey).filter_by(user_id=current_user.id).first()
        if existing:
            flash('You already have an API key. Use "Regenerate" to create a new one.', 'error')
            return redirect(url_for('api_keys.my_key'))

        key_id, raw_secret, full_key = generate_api_key()

        api_key = ApiKey(
            user_id=current_user.id,
            name='Default',
            key_id=key_id,
            key_hash=hash_api_secret(raw_secret),
            scopes=[],  # Empty — admin will assign scopes
            is_active=True,
        )
        db.add(api_key)
        db.commit()

        audit_log(AuditEvent.CONFIG_UPDATED, f"Generated API key (key_id={key_id})")

        response = make_response(render_template('api_keys/created.html',
                               full_key=full_key,
                               key_name='Default',
                               scopes=[]))
        response.headers['Cache-Control'] = 'no-store'
        return response
    except Exception as e:
        db.rollback()
        current_app.logger.error(f"Error generating API key: {e}")
        flash('An error occurred while generating the key.', 'error')
        return redirect(url_for('api_keys.my_key'))
    finally:
        db.close()


@api_keys_bp.route('/regenerate', methods=['POST'])
@login_required
def regenerate_key():
    """Delete the existing key and create a new one."""
    from web.models.api_key import ApiKey, generate_api_key, hash_api_secret

    db = get_session()
    try:
        existing = db.query(ApiKey).filter_by(user_id=current_user.id).first()

        # Preserve scopes and settings from old key
        old_scopes = existing.scopes if existing and existing.scopes else []
        old_rate_limit = existing.rate_limit if existing else 60
        old_daily_quota = existing.daily_quota if existing else 10000

        if existing:
            db.delete(existing)
            db.flush()

        key_id, raw_secret, full_key = generate_api_key()

        api_key = ApiKey(
            user_id=current_user.id,
            name='Default',
            key_id=key_id,
            key_hash=hash_api_secret(raw_secret),
            scopes=old_scopes,
            rate_limit=old_rate_limit,
            daily_quota=old_daily_quota,
            is_active=True,
        )
        db.add(api_key)
        db.commit()

        audit_log(AuditEvent.CONFIG_UPDATED, f"Regenerated API key (new key_id={key_id})")

        response = make_response(render_template('api_keys/created.html',
                               full_key=full_key,
                               key_name='Default',
                               scopes=old_scopes))
        response.headers['Cache-Control'] = 'no-store'
        return response
    except Exception as e:
        db.rollback()
        current_app.logger.error(f"Error regenerating API key: {e}")
        flash('An error occurred while regenerating the key.', 'error')
        return redirect(url_for('api_keys.my_key'))
    finally:
        db.close()


@api_keys_bp.route('/integration-guide')
def integration_guide():
    """
    Integration guide for the recommend + booking API. Accessible to:
      - logged-in admin users (session), OR
      - anyone holding a valid API key, passed via the `X-API-Key` header.

    Header-only on purpose: a `?key=` query param would leak to access
    logs. The shareable artifact for partners is the downloaded PDF /
    HTML / Markdown — admins download via the /api-keys/ page and send
    the file directly (email, Slack, etc.).

    Formats (via `?format=`):
      - `html` (default) — rendered web page
      - `md`             — raw markdown
      - `pdf`            — printable A4 PDF

    `?download=1` forces a Save-As prompt.
    """
    if not _is_doc_request_authorised():
        return jsonify({
            'error': 'Authentication required',
            'message': 'Pass your API key in the X-API-Key header.',
        }), 401

    fmt = (request.args.get('format') or 'html').strip().lower()
    download = bool(request.args.get('download'))

    if fmt == 'md':
        path, mime, filename, is_binary = (
            _INTEGRATION_GUIDE_MD,
            'text/markdown; charset=utf-8',
            'esa_recommendation_api_guide.md',
            False,
        )
    elif fmt == 'pdf':
        path, mime, filename, is_binary = (
            _INTEGRATION_GUIDE_PDF,
            'application/pdf',
            'esa_recommendation_api_guide.pdf',
            True,
        )
    else:
        path, mime, filename, is_binary = (
            _INTEGRATION_GUIDE_HTML,
            'text/html; charset=utf-8',
            'esa_recommendation_api_guide.html',
            False,
        )

    if not path.is_file():
        abort(404)
    try:
        content = path.read_bytes() if is_binary else path.read_text(encoding='utf-8')
    except OSError:
        abort(500)

    resp = make_response(content)
    resp.headers['Content-Type'] = mime
    # Auth-gated doc — don't let intermediaries cache it.
    resp.headers['Cache-Control'] = 'private, max-age=0, must-revalidate'
    resp.headers['X-Content-Type-Options'] = 'nosniff'
    resp.headers['X-Robots-Tag'] = 'noindex, nofollow'
    # PDF / HTML inline display (no script execution).
    if fmt != 'pdf':
        resp.headers['Content-Security-Policy'] = (
            "default-src 'none'; style-src 'unsafe-inline'; "
            "img-src data:; base-uri 'none'; frame-ancestors 'none'"
        )
    if download or fmt == 'pdf':
        disposition = 'attachment' if download else 'inline'
        # RFC 6266 — `filename=` for legacy clients, `filename*=UTF-8''<pct>`
        # for full Unicode support. Filenames are compile-time constants,
        # but encode anyway to keep the pattern injection-safe if they
        # ever become dynamic.
        from urllib.parse import quote as _urlquote
        encoded = _urlquote(filename, safe='')
        resp.headers['Content-Disposition'] = (
            f"{disposition}; filename=\"{filename}\"; "
            f"filename*=UTF-8''{encoded}"
        )
    return resp
