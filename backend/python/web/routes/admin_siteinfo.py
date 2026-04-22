"""
Admin SiteInfo routes - manage the siteinfo table in the PBI database.
Supports dynamic column management; all row queries use raw SQL via text()
so schema changes are reflected immediately without ORM model updates.
"""

import logging
import re

from flask import Blueprint, render_template, redirect, url_for, request, flash
from flask_login import login_required, current_user
from sqlalchemy import text

from web.auth.decorators import config_required
from web.utils.audit import audit_log, AuditEvent

logger = logging.getLogger(__name__)

admin_siteinfo_bp = Blueprint('admin_siteinfo', __name__, url_prefix='/admin/siteinfo')

# ---------------------------------------------------------------------------
# PBI engine (lazy singleton) — canonical during transition to mw_siteinfo
# ---------------------------------------------------------------------------

_pbi_engine = None
_mw_engine = None

# Middleware mirror table — dual-writes run here as best-effort.
# Remove once siteinfo is fully split off PBI.
MW_SITEINFO_TABLE = 'mw_siteinfo'


def _get_pbi_engine():
    global _pbi_engine
    if _pbi_engine is None:
        from common.config_loader import get_database_url
        from sqlalchemy import create_engine
        pbi_url = get_database_url('pbi')
        _pbi_engine = create_engine(pbi_url, pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=300)
    return _pbi_engine


def _get_pbi_session():
    from sqlalchemy.orm import sessionmaker
    Session = sessionmaker(bind=_get_pbi_engine())
    return Session()


def _get_mw_engine():
    global _mw_engine
    if _mw_engine is None:
        from common.config_loader import get_database_url
        from sqlalchemy import create_engine
        mw_url = get_database_url('middleware')
        _mw_engine = create_engine(mw_url, pool_size=3, max_overflow=5, pool_pre_ping=True, pool_recycle=300)
    return _mw_engine


def _mirror_to_mw(sql_template: str, params: dict | None = None) -> str | None:
    """Best-effort dual-write to mw_siteinfo. Returns error string on failure, None on success.

    Does NOT raise — PBI is canonical; caller decides whether to warn the user.
    Pass a SQL string with {table} placeholder for the target table name.
    """
    try:
        ddl = sql_template.format(table=MW_SITEINFO_TABLE)
        with _get_mw_engine().begin() as conn:
            conn.execute(text(ddl), params or {})
        return None
    except Exception as e:
        logger.exception("mw_siteinfo mirror write failed")
        return str(e)[:200]

# ---------------------------------------------------------------------------
# Validation constants
# ---------------------------------------------------------------------------

_COL_NAME_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]{0,62}$')

_ALLOWED_TYPES = {
    'VARCHAR', 'INTEGER', 'NUMERIC', 'BOOLEAN', 'TEXT',
    'DATE', 'TIMESTAMP', 'BIGINT', 'SMALLINT', 'REAL',
}

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _get_table_columns():
    """Return list of dicts describing each column in siteinfo, ordered by position."""
    sql = text("""
        SELECT
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_name = 'siteinfo'
          AND table_schema = 'public'
        ORDER BY ordinal_position
    """)
    session = _get_pbi_session()
    try:
        rows = session.execute(sql).fetchall()
        return [
            {
                'column_name': r.column_name,
                'data_type': r.data_type,
                'character_maximum_length': r.character_maximum_length,
                'numeric_precision': r.numeric_precision,
                'numeric_scale': r.numeric_scale,
                'is_nullable': r.is_nullable,
                'column_default': r.column_default,
            }
            for r in rows
        ]
    finally:
        session.close()


def _get_dependent_views(column_name):
    """Return list of view names that reference the given siteinfo column."""
    sql = text("""
        SELECT view_name
        FROM information_schema.view_column_usage
        WHERE table_name = 'siteinfo'
          AND column_name = :col
    """)
    session = _get_pbi_session()
    try:
        rows = session.execute(sql, {'col': column_name}).fetchall()
        return [r.view_name for r in rows]
    finally:
        session.close()


def _pg_type_to_input(data_type, char_max_length):
    """Map a PostgreSQL data type to an HTML input type string."""
    dt = data_type.lower()
    if dt in ('integer', 'bigint', 'smallint'):
        return 'number'
    if dt in ('character varying', 'text'):
        return 'text'
    if dt in ('numeric', 'decimal', 'real', 'double precision'):
        return 'number'
    if dt == 'boolean':
        return 'checkbox'
    if dt == 'date':
        return 'date'
    if dt.startswith('timestamp'):
        return 'datetime-local'
    return 'text'


def _build_template_dicts(columns):
    """Build input_types and step_attrs dicts for the edit template."""
    input_types = {}
    step_attrs = {}
    for col in columns:
        name = col['column_name']
        dt = col['data_type'].lower()
        input_types[name] = _pg_type_to_input(dt, col['character_maximum_length'])
        if dt in ('numeric', 'decimal', 'real', 'double precision'):
            step_attrs[name] = 'any'
    return input_types, step_attrs


def _build_form_values(columns, site_id=None):
    """
    Parse request.form into a dict of {column_name: value}.
    Skips SiteID when site_id is provided (edit mode).
    Booleans are derived from checkbox presence; empty non-text fields become None.
    """
    values = {}
    for col in columns:
        name = col['column_name']
        if name == 'SiteID' and site_id is not None:
            continue
        if col['data_type'] == 'boolean':
            values[name] = name in request.form
        else:
            raw = request.form.get(name)
            if raw == '' or raw is None:
                values[name] = None
            else:
                values[name] = raw
    return values


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@admin_siteinfo_bp.route('/')
@login_required
@config_required
def list_sites():
    """List all rows in siteinfo."""
    session = _get_pbi_session()
    try:
        columns = _get_table_columns()
        rows = session.execute(text('SELECT * FROM siteinfo ORDER BY "SiteID"')).fetchall()
        sites = [dict(r._mapping) for r in rows]
        # Build column dependency map for schema section
        column_dependencies = {}
        for col in columns:
            deps = _get_dependent_views(col['column_name'])
            if deps:
                column_dependencies[col['column_name']] = deps
        return render_template(
            'admin/siteinfo/list.html',
            sites=sites,
            columns=columns,
            column_dependencies=column_dependencies,
        )
    except Exception:
        logger.exception("Failed to load siteinfo list")
        flash('Failed to load site list.', 'error')
        return redirect(url_for('main.dashboard'))
    finally:
        session.close()


@admin_siteinfo_bp.route('/create', methods=['GET', 'POST'])
@login_required
@config_required
def create_site():
    """Create a new siteinfo row."""
    if request.method == 'POST':
        session = _get_pbi_session()
        try:
            columns = _get_table_columns()
            values = _build_form_values(columns, site_id=None)

            # Filter out columns with None values so defaults/NULLs are handled by DB
            non_null = {k: v for k, v in values.items() if v is not None}

            if not non_null:
                flash('No data provided.', 'error')
                return redirect(url_for('admin_siteinfo.create_site'))

            # col_names come from information_schema, not from request.form keys —
            # safe to use as quoted identifiers. Values are bound as params.
            col_names = ', '.join(f'"{k}"' for k in non_null)
            placeholders = ', '.join(f':{k}' for k in non_null)
            sql = text(f'INSERT INTO siteinfo ({col_names}) VALUES ({placeholders})')
            session.execute(sql, non_null)
            session.commit()

            audit_log(AuditEvent.CONFIG_UPDATED, details='siteinfo: row created', user=current_user.username)

            mirror_err = _mirror_to_mw(
                f'INSERT INTO {{table}} ({col_names}) VALUES ({placeholders}) '
                f'ON CONFLICT ("SiteID") DO NOTHING',
                non_null,
            )
            if mirror_err:
                flash('Site created in PBI; mw_siteinfo mirror failed — see logs.', 'warning')
            else:
                flash('Site created successfully.', 'success')
            return redirect(url_for('admin_siteinfo.list_sites'))
        except Exception:
            logger.exception("Failed to create siteinfo row")
            session.rollback()
            flash('Failed to create site.', 'error')
            return redirect(url_for('admin_siteinfo.create_site'))
        finally:
            session.close()

    # GET
    try:
        columns = _get_table_columns()
        input_types, step_attrs = _build_template_dicts(columns)
        return render_template(
            'admin/siteinfo/edit.html',
            site=None,
            columns=columns,
            input_types=input_types,
            step_attrs=step_attrs,
        )
    except Exception:
        logger.exception("Failed to load siteinfo create form")
        flash('Failed to load form.', 'error')
        return redirect(url_for('admin_siteinfo.list_sites'))


@admin_siteinfo_bp.route('/<int:site_id>/edit', methods=['GET', 'POST'])
@login_required
@config_required
def edit_site(site_id):
    """Edit an existing siteinfo row."""
    if request.method == 'POST':
        session = _get_pbi_session()
        try:
            columns = _get_table_columns()
            values = _build_form_values(columns, site_id=site_id)

            if not values:
                flash('No data to update.', 'error')
                return redirect(url_for('admin_siteinfo.edit_site', site_id=site_id))

            set_clause = ', '.join(f'"{k}" = :{k}' for k in values)
            params = dict(values)
            params['_site_id'] = site_id
            sql = text(f'UPDATE siteinfo SET {set_clause} WHERE "SiteID" = :_site_id')
            session.execute(sql, params)
            session.commit()

            audit_log(AuditEvent.CONFIG_UPDATED,
                      details=f'siteinfo: row updated SiteID={site_id}',
                      user=current_user.username)

            mirror_err = _mirror_to_mw(
                f'UPDATE {{table}} SET {set_clause} WHERE "SiteID" = :_site_id',
                params,
            )
            if mirror_err:
                flash('Site updated in PBI; mw_siteinfo mirror failed — see logs.', 'warning')
            else:
                flash('Site updated successfully.', 'success')
            return redirect(url_for('admin_siteinfo.list_sites'))
        except Exception:
            logger.exception("Failed to update siteinfo row SiteID=%s", site_id)
            session.rollback()
            flash('Failed to update site.', 'error')
            return redirect(url_for('admin_siteinfo.edit_site', site_id=site_id))
        finally:
            session.close()

    # GET
    session = _get_pbi_session()
    try:
        columns = _get_table_columns()
        row = session.execute(
            text('SELECT * FROM siteinfo WHERE "SiteID" = :id'),
            {'id': site_id}
        ).fetchone()
        if row is None:
            flash('Site not found.', 'error')
            return redirect(url_for('admin_siteinfo.list_sites'))
        site = dict(row._mapping)
        input_types, step_attrs = _build_template_dicts(columns)
        return render_template(
            'admin/siteinfo/edit.html',
            site=site,
            columns=columns,
            input_types=input_types,
            step_attrs=step_attrs,
        )
    except Exception:
        logger.exception("Failed to load siteinfo edit form for SiteID=%s", site_id)
        flash('Failed to load site.', 'error')
        return redirect(url_for('admin_siteinfo.list_sites'))
    finally:
        session.close()


@admin_siteinfo_bp.route('/<int:site_id>/delete', methods=['POST'])
@login_required
@config_required
def delete_site(site_id):
    """Delete a siteinfo row."""
    session = _get_pbi_session()
    try:
        session.execute(
            text('DELETE FROM siteinfo WHERE "SiteID" = :id'),
            {'id': site_id}
        )
        session.commit()
        audit_log(AuditEvent.CONFIG_UPDATED,
                  details=f'siteinfo: row deleted SiteID={site_id}',
                  user=current_user.username)

        mirror_err = _mirror_to_mw(
            'DELETE FROM {table} WHERE "SiteID" = :id',
            {'id': site_id},
        )
        if mirror_err:
            flash('Site deleted in PBI; mw_siteinfo mirror failed — see logs.', 'warning')
        else:
            flash('Site deleted successfully.', 'success')
    except Exception:
        logger.exception("Failed to delete siteinfo row SiteID=%s", site_id)
        session.rollback()
        flash('Failed to delete site.', 'error')
    finally:
        session.close()
    return redirect(url_for('admin_siteinfo.list_sites'))


@admin_siteinfo_bp.route('/columns/add', methods=['POST'])
@login_required
@config_required
def add_column():
    """Add a new column to siteinfo via ALTER TABLE."""
    col_name = request.form.get('col_name', '').strip()
    col_type = request.form.get('col_type', '').strip().upper()

    if not _COL_NAME_RE.match(col_name):
        flash('Invalid column name. Use letters, digits, and underscores; must start with a letter or underscore.', 'error')
        return redirect(url_for('admin_siteinfo.list_sites'))

    if col_type not in _ALLOWED_TYPES:
        flash(f'Invalid column type. Allowed: {", ".join(sorted(_ALLOWED_TYPES))}.', 'error')
        return redirect(url_for('admin_siteinfo.list_sites'))

    # Build the type expression — validated inputs only, no user-controlled interpolation
    # beyond the already-validated col_name and col_type.
    if col_type == 'VARCHAR':
        length_raw = request.form.get('col_length', '255').strip()
        try:
            length = int(length_raw)
            if length < 1 or length > 10485760:
                raise ValueError
        except ValueError:
            flash('Invalid VARCHAR length.', 'error')
            return redirect(url_for('admin_siteinfo.list_sites'))
        type_expr = f'VARCHAR({length})'
    elif col_type == 'NUMERIC':
        precision_raw = request.form.get('col_precision', '').strip()
        scale_raw = request.form.get('col_scale', '').strip()
        if precision_raw:
            try:
                precision = int(precision_raw)
                if precision < 1 or precision > 1000:
                    raise ValueError
            except ValueError:
                flash('Invalid NUMERIC precision.', 'error')
                return redirect(url_for('admin_siteinfo.list_sites'))
            if scale_raw:
                try:
                    scale = int(scale_raw)
                    if scale < 0 or scale > precision:
                        raise ValueError
                except ValueError:
                    flash('Invalid NUMERIC scale.', 'error')
                    return redirect(url_for('admin_siteinfo.list_sites'))
                type_expr = f'NUMERIC({precision},{scale})'
            else:
                type_expr = f'NUMERIC({precision})'
        else:
            type_expr = 'NUMERIC'
    else:
        type_expr = col_type

    # col_name is validated by regex; type_expr is constructed from validated integers
    # and a whitelisted keyword — safe to use in DDL.
    ddl = f'ALTER TABLE siteinfo ADD COLUMN "{col_name}" {type_expr}'

    session = _get_pbi_session()
    try:
        session.execute(text(ddl))
        session.commit()
        audit_log(AuditEvent.CONFIG_UPDATED,
                  details=f'siteinfo: column added {col_name} {type_expr}',
                  user=current_user.username)

        mirror_err = _mirror_to_mw(
            f'ALTER TABLE {{table}} ADD COLUMN IF NOT EXISTS "{col_name}" {type_expr}',
        )
        if mirror_err:
            flash(f'Column "{col_name}" added on PBI; mw_siteinfo mirror failed — see logs.', 'warning')
        else:
            flash(f'Column "{col_name}" ({type_expr}) added successfully.', 'success')
    except Exception:
        logger.exception("Failed to add column %s to siteinfo", col_name)
        session.rollback()
        flash('Failed to add column.', 'error')
    finally:
        session.close()

    return redirect(url_for('admin_siteinfo.list_sites'))


@admin_siteinfo_bp.route('/columns/<col_name>/delete', methods=['POST'])
@login_required
@config_required
def delete_column(col_name):
    """Drop a column from siteinfo, blocked if any views depend on it."""
    if not _COL_NAME_RE.match(col_name):
        flash('Invalid column name.', 'error')
        return redirect(url_for('admin_siteinfo.list_sites'))

    # Verify the column actually exists before attempting DDL
    existing = [c['column_name'] for c in _get_table_columns()]
    if col_name not in existing:
        flash('Column not found.', 'error')
        return redirect(url_for('admin_siteinfo.list_sites'))

    dependent_views = _get_dependent_views(col_name)
    if dependent_views:
        view_list = ', '.join(dependent_views)
        flash(
            f'Cannot drop column "{col_name}": it is referenced by view(s): {view_list}.',
            'error'
        )
        return redirect(url_for('admin_siteinfo.list_sites'))

    # col_name validated by regex above — safe to interpolate into DDL.
    ddl = f'ALTER TABLE siteinfo DROP COLUMN "{col_name}"'

    session = _get_pbi_session()
    try:
        session.execute(text(ddl))
        session.commit()
        audit_log(AuditEvent.CONFIG_UPDATED,
                  details=f'siteinfo: column dropped {col_name}',
                  user=current_user.username)

        mirror_err = _mirror_to_mw(
            f'ALTER TABLE {{table}} DROP COLUMN IF EXISTS "{col_name}"',
        )
        if mirror_err:
            flash(f'Column "{col_name}" dropped on PBI; mw_siteinfo mirror failed — see logs.', 'warning')
        else:
            flash(f'Column "{col_name}" dropped successfully.', 'success')
    except Exception:
        logger.exception("Failed to drop column %s from siteinfo", col_name)
        session.rollback()
        flash('Failed to drop column.', 'error')
    finally:
        session.close()

    return redirect(url_for('admin_siteinfo.list_sites'))
