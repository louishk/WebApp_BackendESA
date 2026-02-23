"""
URL Shortener API routes.
Provides link creation, management, redirect handling, and click analytics.
"""

import re
import string
import secrets
import hashlib
from datetime import datetime, timedelta
from urllib.parse import urlparse

from flask import Blueprint, jsonify, request, redirect, current_app, g
from sqlalchemy import desc, func, case

from web.auth.jwt_auth import require_auth
from web.utils.rate_limit import rate_limit_api, get_client_ip

links_bp = Blueprint('links', __name__)

# Short code generation config
SHORT_CODE_LENGTH = 7
SHORT_CODE_CHARS = string.ascii_letters + string.digits  # a-z, A-Z, 0-9
# Reserved codes that cannot be used (common web paths)
RESERVED_CODES = frozenset({
    'api', 'admin', 'login', 'logout', 'health', 'status',
    'static', 'assets', 'auth', 'oauth', 'callback', 'links',
})
# Max URL length to accept
MAX_URL_LENGTH = 8192


def get_session():
    """Get database session from app context."""
    return current_app.get_db_session()


def _generate_short_code(length=SHORT_CODE_LENGTH):
    """Generate a cryptographically random short code."""
    return ''.join(secrets.choice(SHORT_CODE_CHARS) for _ in range(length))


def _validate_url(url):
    """
    Validate that a URL is well-formed and uses an allowed scheme.

    Returns:
        tuple: (is_valid: bool, error_message: str or None)
    """
    if not url or not isinstance(url, str):
        return False, 'URL is required'

    url = url.strip()

    if len(url) > MAX_URL_LENGTH:
        return False, f'URL exceeds maximum length of {MAX_URL_LENGTH} characters'

    try:
        parsed = urlparse(url)
    except ValueError:
        return False, 'Invalid URL format'

    if parsed.scheme not in ('http', 'https'):
        return False, 'URL must use http or https scheme'

    if not parsed.netloc:
        return False, 'URL must include a domain'

    # Basic domain validation
    if '.' not in parsed.netloc and parsed.netloc != 'localhost':
        return False, 'URL domain appears invalid'

    return True, None


def _validate_short_code(code):
    """
    Validate a custom short code.

    Returns:
        tuple: (is_valid: bool, error_message: str or None)
    """
    if not code or not isinstance(code, str):
        return False, 'Short code is required'

    if len(code) < 3 or len(code) > 20:
        return False, 'Short code must be 3-20 characters'

    if not re.match(r'^[a-zA-Z0-9_-]+$', code):
        return False, 'Short code may only contain letters, numbers, hyphens, and underscores'

    if code.lower() in RESERVED_CODES:
        return False, f'Short code "{code}" is reserved'

    return True, None


def _parse_user_agent(ua_string):
    """
    Parse user-agent string into device_type, browser, and OS.
    Lightweight parsing without external dependencies.
    """
    if not ua_string:
        return 'unknown', 'unknown', 'unknown'

    ua = ua_string.lower()

    # Detect bots
    bot_patterns = ['bot', 'crawler', 'spider', 'slurp', 'fetch', 'curl', 'wget', 'python-requests']
    if any(p in ua for p in bot_patterns):
        return 'bot', 'bot', 'bot'

    # Device type
    if any(t in ua for t in ['iphone', 'android', 'mobile', 'windows phone']):
        device = 'mobile'
    elif any(t in ua for t in ['ipad', 'tablet', 'kindle']):
        device = 'tablet'
    else:
        device = 'desktop'

    # Browser detection
    if 'edg/' in ua or 'edge/' in ua:
        browser = 'Edge'
    elif 'opr/' in ua or 'opera' in ua:
        browser = 'Opera'
    elif 'chrome/' in ua and 'safari/' in ua:
        browser = 'Chrome'
    elif 'firefox/' in ua:
        browser = 'Firefox'
    elif 'safari/' in ua:
        browser = 'Safari'
    elif 'msie' in ua or 'trident' in ua:
        browser = 'IE'
    else:
        browser = 'Other'

    # OS detection
    if 'windows' in ua:
        os_name = 'Windows'
    elif 'mac os' in ua or 'macos' in ua:
        os_name = 'macOS'
    elif 'iphone' in ua or 'ipad' in ua:
        os_name = 'iOS'
    elif 'android' in ua:
        os_name = 'Android'
    elif 'linux' in ua:
        os_name = 'Linux'
    else:
        os_name = 'Other'

    return device, browser, os_name


def _get_username():
    """Get username from authenticated request context."""
    if hasattr(g, 'current_user') and g.current_user:
        return g.current_user.get('sub', 'unknown')
    return 'unknown'


# =============================================================================
# Public: Redirect endpoint (no auth required)
# =============================================================================

@links_bp.route('/s/<short_code>')
def redirect_short_link(short_code):
    """
    Redirect a short link to its original URL.
    Tracks the click asynchronously.
    This is the public-facing endpoint - no authentication required.
    """
    from web.models.short_link import ShortLink, LinkClick

    session = get_session()
    try:
        link = session.query(ShortLink).filter_by(short_code=short_code).first()

        if not link:
            return jsonify({'error': 'Link not found'}), 404

        if not link.is_accessible():
            if not link.is_active:
                return jsonify({'error': 'This link has been deactivated'}), 410
            if link.is_expired():
                return jsonify({'error': 'This link has expired'}), 410
            if link.is_click_capped():
                return jsonify({'error': 'This link has reached its click limit'}), 410

        # Check password protection
        if link.password_hash:
            provided = request.args.get('key') or request.headers.get('X-Link-Key')
            if not provided:
                return jsonify({
                    'error': 'This link is password-protected',
                    'hint': 'Provide the key via ?key= query param or X-Link-Key header'
                }), 401
            if hashlib.sha256(provided.encode()).hexdigest() != link.password_hash:
                return jsonify({'error': 'Invalid link key'}), 403

        original_url = link.original_url

        # Record click
        ip = get_client_ip()
        ua_string = request.headers.get('User-Agent', '')
        device, browser, os_name = _parse_user_agent(ua_string)

        click = LinkClick(
            link_id=link.id,
            ip_address=ip,
            user_agent=ua_string[:500] if ua_string else None,
            referer=request.referrer[:2000] if request.referrer else None,
            device_type=device,
            browser=browser,
            os=os_name,
        )
        session.add(click)

        # Update counters
        link.total_clicks = (link.total_clicks or 0) + 1

        # Check if this IP has clicked this link before for unique tracking
        existing_click = session.query(LinkClick).filter_by(
            link_id=link.id, ip_address=ip
        ).count()
        if existing_click <= 1:  # This is the first click (the one we just added)
            link.unique_clicks = (link.unique_clicks or 0) + 1

        session.commit()

        return redirect(original_url, code=302)

    except Exception as e:
        session.rollback()
        current_app.logger.error(f"Error redirecting short link {short_code}: {e}")
        return jsonify({'error': 'Internal server error'}), 500
    finally:
        session.close()


# =============================================================================
# API: Link Management (auth required)
# =============================================================================

@links_bp.route('/api/links', methods=['POST'])
@require_auth
@rate_limit_api(max_requests=30, window_seconds=60)
def create_link():
    """
    Create a new shortened link.

    Request body:
        url (required): The original URL to shorten
        custom_code (optional): Custom short code (3-20 chars, alphanumeric/-/_)
        title (optional): Human-readable title
        tags (optional): List of tags for organization
        expires_at (optional): ISO 8601 expiry datetime
        password (optional): Password to protect the link
        max_clicks (optional): Maximum number of clicks before link deactivates
    """
    from web.models.short_link import ShortLink

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body is required'}), 400

    # Validate URL
    url = data.get('url', '').strip()
    is_valid, error = _validate_url(url)
    if not is_valid:
        return jsonify({'error': error}), 400

    # Handle custom short code or generate one
    custom_code = data.get('custom_code', '').strip() if data.get('custom_code') else None

    session = get_session()
    try:
        if custom_code:
            is_valid, error = _validate_short_code(custom_code)
            if not is_valid:
                return jsonify({'error': error}), 400

            # Check if code already exists
            existing = session.query(ShortLink).filter_by(short_code=custom_code).first()
            if existing:
                return jsonify({'error': f'Short code "{custom_code}" is already taken'}), 409
            short_code = custom_code
        else:
            # Generate a unique random code
            for _ in range(10):
                short_code = _generate_short_code()
                existing = session.query(ShortLink).filter_by(short_code=short_code).first()
                if not existing:
                    break
            else:
                return jsonify({'error': 'Failed to generate unique short code. Try again.'}), 500

        # Parse optional fields
        title = data.get('title', '').strip()[:255] if data.get('title') else None

        tags = data.get('tags')
        tags_str = None
        if tags:
            if isinstance(tags, list):
                # Sanitize: strip whitespace, remove empty, limit to 10 tags
                clean_tags = [t.strip()[:50] for t in tags if t.strip()][:10]
                tags_str = ','.join(clean_tags)
            elif isinstance(tags, str):
                tags_str = tags.strip()[:500]

        expires_at = None
        if data.get('expires_at'):
            try:
                expires_at = datetime.fromisoformat(data['expires_at'].replace('Z', '+00:00'))
                if expires_at.tzinfo:
                    expires_at = expires_at.replace(tzinfo=None)
                if expires_at <= datetime.utcnow():
                    return jsonify({'error': 'Expiry date must be in the future'}), 400
            except (ValueError, AttributeError):
                return jsonify({'error': 'Invalid expires_at format. Use ISO 8601.'}), 400

        password_hash = None
        if data.get('password'):
            pw = data['password']
            if len(pw) < 3 or len(pw) > 128:
                return jsonify({'error': 'Password must be 3-128 characters'}), 400
            password_hash = hashlib.sha256(pw.encode()).hexdigest()

        max_clicks = None
        if data.get('max_clicks') is not None:
            try:
                max_clicks = int(data['max_clicks'])
                if max_clicks < 1:
                    return jsonify({'error': 'max_clicks must be at least 1'}), 400
            except (ValueError, TypeError):
                return jsonify({'error': 'max_clicks must be an integer'}), 400

        link = ShortLink(
            short_code=short_code,
            original_url=url,
            title=title,
            tags=tags_str,
            expires_at=expires_at,
            password_hash=password_hash,
            max_clicks=max_clicks,
            created_by=_get_username(),
        )
        session.add(link)
        session.commit()

        result = link.to_dict()
        # Include the full short URL for convenience
        result['short_url'] = f"{request.host_url}s/{short_code}"

        return jsonify(result), 201

    except Exception as e:
        session.rollback()
        current_app.logger.error(f"Error creating short link: {e}")
        return jsonify({'error': 'Failed to create link'}), 500
    finally:
        session.close()


@links_bp.route('/api/links', methods=['GET'])
@require_auth
@rate_limit_api(max_requests=60, window_seconds=60)
def list_links():
    """
    List shortened links with filtering and pagination.

    Query params:
        search: Search in title, short_code, original_url
        tag: Filter by tag
        is_active: Filter by active status (true/false)
        created_by: Filter by creator
        sort: Sort field (created_at, total_clicks, title) - default created_at
        order: Sort order (asc, desc) - default desc
        limit: Results per page (default 50, max 200)
        offset: Pagination offset (default 0)
    """
    from web.models.short_link import ShortLink

    session = get_session()
    try:
        query = session.query(ShortLink)

        # Filters
        search = request.args.get('search', '').strip()
        if search:
            search_pattern = f'%{search}%'
            query = query.filter(
                (ShortLink.short_code.ilike(search_pattern)) |
                (ShortLink.original_url.ilike(search_pattern)) |
                (ShortLink.title.ilike(search_pattern))
            )

        tag = request.args.get('tag', '').strip()
        if tag:
            query = query.filter(ShortLink.tags.ilike(f'%{tag}%'))

        is_active = request.args.get('is_active')
        if is_active is not None:
            query = query.filter(ShortLink.is_active == (is_active.lower() == 'true'))

        created_by = request.args.get('created_by', '').strip()
        if created_by:
            query = query.filter(ShortLink.created_by == created_by)

        # Count total before pagination
        total = query.count()

        # Sorting
        sort_field = request.args.get('sort', 'created_at')
        sort_order = request.args.get('order', 'desc')

        sort_map = {
            'created_at': ShortLink.created_at,
            'total_clicks': ShortLink.total_clicks,
            'title': ShortLink.title,
            'short_code': ShortLink.short_code,
        }
        sort_col = sort_map.get(sort_field, ShortLink.created_at)
        if sort_order == 'asc':
            query = query.order_by(sort_col.asc())
        else:
            query = query.order_by(sort_col.desc())

        # Pagination
        try:
            limit = min(int(request.args.get('limit', 50)), 200)
        except (ValueError, TypeError):
            limit = 50
        try:
            offset = int(request.args.get('offset', 0))
        except (ValueError, TypeError):
            offset = 0

        links = query.offset(offset).limit(limit).all()

        return jsonify({
            'total': total,
            'offset': offset,
            'limit': limit,
            'links': [link.to_dict() for link in links],
        })

    finally:
        session.close()


@links_bp.route('/api/links/<int:link_id>', methods=['GET'])
@require_auth
def get_link(link_id):
    """Get a single link by ID with full details."""
    from web.models.short_link import ShortLink

    session = get_session()
    try:
        link = session.query(ShortLink).filter_by(id=link_id).first()
        if not link:
            return jsonify({'error': 'Link not found'}), 404

        result = link.to_dict()
        result['short_url'] = f"{request.host_url}s/{link.short_code}"
        return jsonify(result)
    finally:
        session.close()


@links_bp.route('/api/links/<int:link_id>', methods=['PUT'])
@require_auth
@rate_limit_api(max_requests=30, window_seconds=60)
def update_link(link_id):
    """
    Update a shortened link.

    Updatable fields: title, tags, is_active, expires_at, password, max_clicks, original_url
    """
    from web.models.short_link import ShortLink

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body is required'}), 400

    session = get_session()
    try:
        link = session.query(ShortLink).filter_by(id=link_id).first()
        if not link:
            return jsonify({'error': 'Link not found'}), 404

        if 'url' in data:
            is_valid, error = _validate_url(data['url'])
            if not is_valid:
                return jsonify({'error': error}), 400
            link.original_url = data['url'].strip()

        if 'title' in data:
            link.title = data['title'].strip()[:255] if data['title'] else None

        if 'tags' in data:
            tags = data['tags']
            if isinstance(tags, list):
                clean_tags = [t.strip()[:50] for t in tags if t.strip()][:10]
                link.tags = ','.join(clean_tags) if clean_tags else None
            elif isinstance(tags, str):
                link.tags = tags.strip()[:500] or None
            else:
                link.tags = None

        if 'is_active' in data:
            link.is_active = bool(data['is_active'])

        if 'expires_at' in data:
            if data['expires_at'] is None:
                link.expires_at = None
            else:
                try:
                    link.expires_at = datetime.fromisoformat(
                        data['expires_at'].replace('Z', '+00:00')
                    )
                    if link.expires_at.tzinfo:
                        link.expires_at = link.expires_at.replace(tzinfo=None)
                except (ValueError, AttributeError):
                    return jsonify({'error': 'Invalid expires_at format'}), 400

        if 'password' in data:
            if data['password'] is None or data['password'] == '':
                link.password_hash = None
            else:
                pw = data['password']
                if len(pw) < 3 or len(pw) > 128:
                    return jsonify({'error': 'Password must be 3-128 characters'}), 400
                link.password_hash = hashlib.sha256(pw.encode()).hexdigest()

        if 'max_clicks' in data:
            if data['max_clicks'] is None:
                link.max_clicks = None
            else:
                try:
                    link.max_clicks = int(data['max_clicks'])
                    if link.max_clicks < 1:
                        return jsonify({'error': 'max_clicks must be at least 1'}), 400
                except (ValueError, TypeError):
                    return jsonify({'error': 'max_clicks must be an integer'}), 400

        link.updated_at = datetime.utcnow()
        session.commit()

        result = link.to_dict()
        result['short_url'] = f"{request.host_url}s/{link.short_code}"
        return jsonify(result)

    except Exception as e:
        session.rollback()
        current_app.logger.error(f"Error updating link {link_id}: {e}")
        return jsonify({'error': 'Failed to update link'}), 500
    finally:
        session.close()


@links_bp.route('/api/links/<int:link_id>', methods=['DELETE'])
@require_auth
@rate_limit_api(max_requests=20, window_seconds=60)
def delete_link(link_id):
    """Delete a shortened link and all its click data."""
    from web.models.short_link import ShortLink

    session = get_session()
    try:
        link = session.query(ShortLink).filter_by(id=link_id).first()
        if not link:
            return jsonify({'error': 'Link not found'}), 404

        session.delete(link)
        session.commit()

        return jsonify({'success': True, 'message': f'Link {link.short_code} deleted'})

    except Exception as e:
        session.rollback()
        current_app.logger.error(f"Error deleting link {link_id}: {e}")
        return jsonify({'error': 'Failed to delete link'}), 500
    finally:
        session.close()


@links_bp.route('/api/links/bulk', methods=['POST'])
@require_auth
@rate_limit_api(max_requests=10, window_seconds=60)
def bulk_create_links():
    """
    Create multiple shortened links at once.

    Request body:
        urls: List of objects with {url, title?, tags?, custom_code?}
    """
    from web.models.short_link import ShortLink

    data = request.get_json()
    if not data or 'urls' not in data:
        return jsonify({'error': 'urls array is required'}), 400

    urls = data['urls']
    if not isinstance(urls, list) or len(urls) == 0:
        return jsonify({'error': 'urls must be a non-empty array'}), 400

    if len(urls) > 100:
        return jsonify({'error': 'Maximum 100 URLs per bulk request'}), 400

    session = get_session()
    try:
        results = []
        errors = []
        username = _get_username()

        for i, item in enumerate(urls):
            url = item.get('url', '').strip() if isinstance(item, dict) else str(item).strip()

            is_valid, error = _validate_url(url)
            if not is_valid:
                errors.append({'index': i, 'url': url, 'error': error})
                continue

            custom_code = item.get('custom_code') if isinstance(item, dict) else None
            if custom_code:
                is_valid, error = _validate_short_code(custom_code)
                if not is_valid:
                    errors.append({'index': i, 'url': url, 'error': error})
                    continue
                existing = session.query(ShortLink).filter_by(short_code=custom_code).first()
                if existing:
                    errors.append({'index': i, 'url': url, 'error': f'Code "{custom_code}" taken'})
                    continue
                short_code = custom_code
            else:
                short_code = _generate_short_code()
                # Retry if collision
                for _ in range(5):
                    if not session.query(ShortLink).filter_by(short_code=short_code).first():
                        break
                    short_code = _generate_short_code()

            title = item.get('title', '').strip()[:255] if isinstance(item, dict) and item.get('title') else None

            tags_str = None
            if isinstance(item, dict) and item.get('tags'):
                tags = item['tags']
                if isinstance(tags, list):
                    clean_tags = [t.strip()[:50] for t in tags if t.strip()][:10]
                    tags_str = ','.join(clean_tags)

            link = ShortLink(
                short_code=short_code,
                original_url=url,
                title=title,
                tags=tags_str,
                created_by=username,
            )
            session.add(link)
            results.append({
                'url': url,
                'short_code': short_code,
                'short_url': f"{request.host_url}s/{short_code}",
                'title': title,
            })

        session.commit()

        return jsonify({
            'created': len(results),
            'errors': len(errors),
            'links': results,
            'error_details': errors if errors else None,
        }), 201

    except Exception as e:
        session.rollback()
        current_app.logger.error(f"Error in bulk link creation: {e}")
        return jsonify({'error': 'Failed to create links'}), 500
    finally:
        session.close()


# =============================================================================
# API: Analytics
# =============================================================================

@links_bp.route('/api/links/<int:link_id>/clicks', methods=['GET'])
@require_auth
@rate_limit_api(max_requests=30, window_seconds=60)
def get_link_clicks(link_id):
    """
    Get click history for a specific link.

    Query params:
        limit: Max results (default 100, max 500)
        offset: Pagination offset
        since: ISO 8601 date to filter clicks after
    """
    from web.models.short_link import ShortLink, LinkClick

    session = get_session()
    try:
        link = session.query(ShortLink).filter_by(id=link_id).first()
        if not link:
            return jsonify({'error': 'Link not found'}), 404

        query = session.query(LinkClick).filter_by(link_id=link_id)

        since = request.args.get('since')
        if since:
            try:
                since_date = datetime.fromisoformat(since.replace('Z', '+00:00'))
                if since_date.tzinfo:
                    since_date = since_date.replace(tzinfo=None)
                query = query.filter(LinkClick.clicked_at >= since_date)
            except (ValueError, AttributeError):
                return jsonify({'error': 'Invalid since format'}), 400

        total = query.count()

        try:
            limit = min(int(request.args.get('limit', 100)), 500)
        except (ValueError, TypeError):
            limit = 100
        try:
            offset = int(request.args.get('offset', 0))
        except (ValueError, TypeError):
            offset = 0

        clicks = query.order_by(desc(LinkClick.clicked_at)).offset(offset).limit(limit).all()

        return jsonify({
            'link_id': link_id,
            'short_code': link.short_code,
            'total': total,
            'offset': offset,
            'limit': limit,
            'clicks': [c.to_dict() for c in clicks],
        })

    finally:
        session.close()


@links_bp.route('/api/links/<int:link_id>/analytics', methods=['GET'])
@require_auth
@rate_limit_api(max_requests=30, window_seconds=60)
def get_link_analytics(link_id):
    """
    Get aggregated analytics for a specific link.

    Query params:
        period: 1d, 7d, 30d, 90d, all (default 30d)

    Returns breakdowns by:
        - Time (clicks over time)
        - Device type
        - Browser
        - OS
        - Top referrers
        - Top countries
    """
    from web.models.short_link import ShortLink, LinkClick

    period = request.args.get('period', '30d')
    days_map = {'1d': 1, '7d': 7, '30d': 30, '90d': 90, 'all': None}
    days = days_map.get(period)

    session = get_session()
    try:
        link = session.query(ShortLink).filter_by(id=link_id).first()
        if not link:
            return jsonify({'error': 'Link not found'}), 404

        base_query = session.query(LinkClick).filter_by(link_id=link_id)
        if days is not None:
            since = datetime.utcnow() - timedelta(days=days)
            base_query = base_query.filter(LinkClick.clicked_at >= since)

        # Clicks over time
        trunc_unit = 'hour' if period == '1d' else 'day'
        timeline = session.query(
            func.date_trunc(trunc_unit, LinkClick.clicked_at).label('bucket'),
            func.count(LinkClick.id).label('count'),
        ).filter(
            LinkClick.link_id == link_id,
            *([LinkClick.clicked_at >= since] if days else []),
        ).group_by('bucket').order_by('bucket').all()

        # Device breakdown
        devices = session.query(
            LinkClick.device_type,
            func.count(LinkClick.id).label('count'),
        ).filter(
            LinkClick.link_id == link_id,
            *([LinkClick.clicked_at >= since] if days else []),
        ).group_by(LinkClick.device_type).order_by(desc('count')).all()

        # Browser breakdown
        browsers = session.query(
            LinkClick.browser,
            func.count(LinkClick.id).label('count'),
        ).filter(
            LinkClick.link_id == link_id,
            *([LinkClick.clicked_at >= since] if days else []),
        ).group_by(LinkClick.browser).order_by(desc('count')).all()

        # OS breakdown
        os_stats = session.query(
            LinkClick.os,
            func.count(LinkClick.id).label('count'),
        ).filter(
            LinkClick.link_id == link_id,
            *([LinkClick.clicked_at >= since] if days else []),
        ).group_by(LinkClick.os).order_by(desc('count')).all()

        # Top referrers
        referrers = session.query(
            LinkClick.referer,
            func.count(LinkClick.id).label('count'),
        ).filter(
            LinkClick.link_id == link_id,
            LinkClick.referer.isnot(None),
            LinkClick.referer != '',
            *([LinkClick.clicked_at >= since] if days else []),
        ).group_by(LinkClick.referer).order_by(desc('count')).limit(20).all()

        # Top countries
        countries = session.query(
            LinkClick.country,
            func.count(LinkClick.id).label('count'),
        ).filter(
            LinkClick.link_id == link_id,
            LinkClick.country.isnot(None),
            *([LinkClick.clicked_at >= since] if days else []),
        ).group_by(LinkClick.country).order_by(desc('count')).limit(20).all()

        period_clicks = base_query.count()
        period_unique = session.query(
            func.count(func.distinct(LinkClick.ip_address))
        ).filter(
            LinkClick.link_id == link_id,
            *([LinkClick.clicked_at >= since] if days else []),
        ).scalar() or 0

        return jsonify({
            'link_id': link_id,
            'short_code': link.short_code,
            'period': period,
            'total_clicks': link.total_clicks,
            'unique_clicks': link.unique_clicks,
            'period_clicks': period_clicks,
            'period_unique_clicks': period_unique,
            'timeline': [
                {'date': t.bucket.isoformat() if t.bucket else None, 'clicks': t.count}
                for t in timeline
            ],
            'devices': [
                {'device': d.device_type or 'unknown', 'clicks': d.count}
                for d in devices
            ],
            'browsers': [
                {'browser': b.browser or 'unknown', 'clicks': b.count}
                for b in browsers
            ],
            'operating_systems': [
                {'os': o.os or 'unknown', 'clicks': o.count}
                for o in os_stats
            ],
            'referrers': [
                {'referer': r.referer, 'clicks': r.count}
                for r in referrers
            ],
            'countries': [
                {'country': c.country or 'unknown', 'clicks': c.count}
                for c in countries
            ],
        })

    finally:
        session.close()


@links_bp.route('/api/links/analytics/summary', methods=['GET'])
@require_auth
@rate_limit_api(max_requests=30, window_seconds=60)
def get_links_summary():
    """
    Get overall URL shortener summary analytics.

    Query params:
        period: 1d, 7d, 30d, 90d (default 7d)
    """
    from web.models.short_link import ShortLink, LinkClick

    period = request.args.get('period', '7d')
    days = {'1d': 1, '7d': 7, '30d': 30, '90d': 90}.get(period, 7)
    since = datetime.utcnow() - timedelta(days=days)

    session = get_session()
    try:
        total_links = session.query(func.count(ShortLink.id)).scalar() or 0
        active_links = session.query(func.count(ShortLink.id)).filter(
            ShortLink.is_active == True
        ).scalar() or 0

        period_clicks = session.query(func.count(LinkClick.id)).filter(
            LinkClick.clicked_at >= since
        ).scalar() or 0

        period_unique = session.query(
            func.count(func.distinct(LinkClick.ip_address))
        ).filter(
            LinkClick.clicked_at >= since
        ).scalar() or 0

        links_created = session.query(func.count(ShortLink.id)).filter(
            ShortLink.created_at >= since
        ).scalar() or 0

        # Top performing links
        top_links = session.query(
            ShortLink.id,
            ShortLink.short_code,
            ShortLink.title,
            ShortLink.original_url,
            func.count(LinkClick.id).label('click_count'),
        ).join(
            LinkClick, LinkClick.link_id == ShortLink.id
        ).filter(
            LinkClick.clicked_at >= since
        ).group_by(
            ShortLink.id, ShortLink.short_code, ShortLink.title, ShortLink.original_url
        ).order_by(desc('click_count')).limit(10).all()

        # Clicks per day timeline
        trunc_unit = 'hour' if period == '1d' else 'day'
        timeline = session.query(
            func.date_trunc(trunc_unit, LinkClick.clicked_at).label('bucket'),
            func.count(LinkClick.id).label('count'),
        ).filter(
            LinkClick.clicked_at >= since
        ).group_by('bucket').order_by('bucket').all()

        return jsonify({
            'period': period,
            'total_links': total_links,
            'active_links': active_links,
            'links_created_in_period': links_created,
            'period_clicks': period_clicks,
            'period_unique_clicks': period_unique,
            'top_links': [
                {
                    'id': t.id,
                    'short_code': t.short_code,
                    'title': t.title,
                    'original_url': t.original_url[:100],
                    'clicks': t.click_count,
                }
                for t in top_links
            ],
            'timeline': [
                {'date': t.bucket.isoformat() if t.bucket else None, 'clicks': t.count}
                for t in timeline
            ],
        })

    finally:
        session.close()


@links_bp.route('/api/links/<int:link_id>/qr', methods=['GET'])
@require_auth
def get_link_qr(link_id):
    """
    Get QR code data for a short link.
    Returns the short URL that can be used to generate a QR code client-side.
    """
    from web.models.short_link import ShortLink

    session = get_session()
    try:
        link = session.query(ShortLink).filter_by(id=link_id).first()
        if not link:
            return jsonify({'error': 'Link not found'}), 404

        short_url = f"{request.host_url}s/{link.short_code}"

        return jsonify({
            'link_id': link_id,
            'short_code': link.short_code,
            'short_url': short_url,
            'title': link.title,
        })
    finally:
        session.close()
