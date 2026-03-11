"""
Gravity Forms WordPress to PostgreSQL Pipeline

Extracts all Gravity Forms submissions from Kinsta WordPress instances
and loads into esa_pbi PostgreSQL for analytics/archival before site decommission.

Instances: SG (13306), MY (13307), KR (13308), HK (13309)
Source: MySQL via SSH tunnel (kinsta-ssh-tunnel*.service)
  - New schema: wp_gf_entry + wp_gf_entry_meta
  - Legacy schema: wp_rg_lead + wp_rg_lead_detail (SG/MY/KR only)

Target: esa_pbi PostgreSQL
  - gf_dim_facility, gf_dim_storage_size, gf_dim_storage_duration
  - gf_entries (wide flat table)
  - gf_entry_meta_raw (EAV backup)

Usage:
    python gravityforms_to_sql.py --instance sg --mode dimensions
    python gravityforms_to_sql.py --instance sg --mode full
    python gravityforms_to_sql.py --instance all --mode full
    python gravityforms_to_sql.py --instance hk --mode full --form-id 8
    python gravityforms_to_sql.py --instance sg --mode full --resume-from 150000
    python gravityforms_to_sql.py --instance sg --mode verify

Dependencies:
    - pymysql (MySQL dialect for SQLAlchemy)
    - SSH tunnels: systemctl --user start kinsta-ssh-tunnel{,-my,-kr,-hk}.service
"""

import argparse
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from tqdm import tqdm

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from common import DataLayerConfig, create_engine_from_config
from common.config import DatabaseConfig, DatabaseType

from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

CHUNK_SIZE = 2000  # Entries per batch

# ============================================================================
# Instance configurations
# ============================================================================
INSTANCES = {
    'sg': {
        'source_instance': 'kinsta_sg',
        'host': '127.0.0.1',
        'port': 13306,
        'database': 'extraspaceasiasg',
        'username': 'extraspaceasiasg',
        'password_env': 'KINSTA_SG_MYSQL_PASSWORD',
    },
    'my': {
        'source_instance': 'kinsta_my',
        'host': '127.0.0.1',
        'port': 13307,
        'database': 'extraspaceasiamy',
        'username': 'extraspaceasiamy',
        'password_env': 'KINSTA_MY_MYSQL_PASSWORD',
    },
    'kr': {
        'source_instance': 'kinsta_kr',
        'host': '127.0.0.1',
        'port': 13308,
        'database': 'extraspaceasiakr',
        'username': 'extraspaceasiakr',
        'password_env': 'KINSTA_KR_MYSQL_PASSWORD',
    },
    'hk': {
        'source_instance': 'kinsta_hk',
        'host': '127.0.0.1',
        'port': 13309,
        'database': 'extraspaceasiahk',
        'username': 'extraspaceasiahk',
        'password_env': 'KINSTA_HK_MYSQL_PASSWORD',
    },
}

# MySQL passwords per instance (can be overridden by env vars or --mysql-password)
MYSQL_PASSWORDS = {
    'sg': 'Kk6FTjffA86SZ0s',
    'my': '5kEu0Nyg84HTsmB',
    'kr': 'PMI8sk97nrZ85TG',
    'hk': 'oJBW87YsgiBiusB',
}

# ============================================================================
# Form field mappings per instance
# form_id -> {meta_key: column_name}
# Only numeric meta_keys (field IDs) are mapped; system keys are skipped.
# ============================================================================

# Common quote form mapping (fields 5/6/7/11/2/3/4/8/9/10/12-15)
# Used by SG Form 8, MY Form 6, KR Form 9, HK Forms 8 & 10
_QUOTE_FORM_MAP = {
    '11': 'salutation', '2': 'name', '3': 'email', '4': 'phone',
    '5': 'facility_id', '6': 'storage_size_id', '7': 'storage_duration_id',
    '8': 'source_channel', '9': 'promo_code', '10': 'unit_type',
    '12': 'utm_source', '13': 'utm_campaign', '14': 'utm_medium',
    '15': 'utm_additional',
}

# Common old quote form mapping (forms 6/7/9 in SG, 5 in MY/KR)
_OLD_QUOTE_FORM_MAP = {
    '5': 'facility_id', '6': 'storage_size_id', '7': 'storage_duration_id',
    '2': 'name', '3': 'email', '4': 'phone', '11': 'salutation',
}

# Contact Us mapping (form 2 in all instances)
_CONTACT_US_MAP = {
    '1': 'name', '2': 'email', '5': 'message', '8': 'phone',
    '9': 'preferred_contact', '10': 'existing_customer',
}

# Payment form mapping (SG form 3, MY form 3)
_PAYMENT_FORM_MAP = {
    '10': 'invoice_number', '18': 'payment_amount', '11': 'payment_due_date',
    '12': 'registered_name', '15': 'email', '16': 'phone',
    '17': 'facility_name',
}

INSTANCE_FORM_FIELD_MAPS = {
    'sg': {
        8: _QUOTE_FORM_MAP,
        2: _CONTACT_US_MAP,
        3: _PAYMENT_FORM_MAP,
        6: _OLD_QUOTE_FORM_MAP,  # Get Quote - Store Locator
        7: _OLD_QUOTE_FORM_MAP,  # Instant Quote
        9: _OLD_QUOTE_FORM_MAP,  # Get a Quote - 10yr Anniversary
        10: {  # Executive Storage SG
            '1': 'name', '2': 'email', '8': 'phone',
            '12': 'package_selection', '5': 'message',
        },
        12: {  # Enquire e-Move
            '1': 'name', '4': 'phone', '5': 'email',
            '14': 'service_type', '8': 'moving_from_postal', '9': 'moving_to_postal',
            '11': 'move_date',
        },
        14: {  # Extra Space Express
            '1': 'name', '2': 'email', '8': 'phone', '10': 'storage_purpose',
        },
        15: {  # Extra Space Express (Commonwealth)
            '1': 'name', '2': 'email', '8': 'phone', '10': 'storage_purpose',
        },
        16: {  # Bizplus Contact Us
            '1': 'name', '5': 'email', '6': 'phone', '7': 'company',
            '8': 'seats_required', '3': 'storage_duration_id',
            '11': 'coworking_solution', '13': 'tour_date', '9': 'promo_code',
        },
        5: {},   # Secure Payment Form - Test
        11: {},  # Get a Quote - Test SugarCRM
        13: {},  # Payment Secure Payment Form
    },
    'my': {
        6: _QUOTE_FORM_MAP,     # Get a Quote - New (MY's main quote form)
        2: _CONTACT_US_MAP,
        3: _PAYMENT_FORM_MAP,   # Secure Payment Form (18.9K entries)
        8: _OLD_QUOTE_FORM_MAP, # Instant Quote
        1: {},                  # Get a Quote (old, fields 13-91, complex)
        7: _OLD_QUOTE_FORM_MAP, # Store Locator
        5: _OLD_QUOTE_FORM_MAP, # Get a Quote - 10yr Anniversary
        9: {},                  # Stripe Payment Secure Payment Form
    },
    'kr': {
        9: _QUOTE_FORM_MAP,     # Get a Quote - New (KR's main quote form)
        2: _CONTACT_US_MAP,
        1: {},                  # Get a Quote (old, fields 13-91, complex)
        5: _OLD_QUOTE_FORM_MAP, # Get a Quote - 10yr Anniversary
        3: {},                  # Secure Payment Form (0 entries)
    },
    'hk': {
        8: _QUOTE_FORM_MAP,     # Get a Quote - New
        10: _QUOTE_FORM_MAP,    # Get a Quote - Home Page (same fields as form 8)
        2: _CONTACT_US_MAP,
        12: {                   # Payment Secure Payment Form (Stripe)
            '12': 'registered_name', '15': 'email', '16': 'phone',
            '5': 'facility_name', '14': 'invoice_number',
        },
        6: _OLD_QUOTE_FORM_MAP, # Get Quote - Store Locator (3 entries)
        9: {},                  # Get a Quote - 10yr Anniversary (0 entries)
        5: {},                  # Secure Payment Form - Test (0 entries)
        1: {},                  # Get a Quote (0 entries)
        11: {},                 # Contact Us (1) (0 entries)
        7: {},                  # Instant Quote (0 entries)
        3: {},                  # Secure Payment Form (0 entries)
    },
}

# Columns that hold integer IDs (need casting for dim lookups)
ID_COLUMNS = {'facility_id', 'storage_size_id', 'storage_duration_id'}

# All valid column names for gf_entries (excluding PKs and ETL metadata)
ENTRY_COLUMNS = {
    'salutation', 'name', 'email', 'phone', 'company',
    'facility_id', 'facility_name', 'storage_size_id', 'storage_size_label',
    'storage_duration_id', 'storage_duration_label',
    'source_channel', 'promo_code', 'unit_type',
    'utm_source', 'utm_campaign', 'utm_medium', 'utm_additional',
    'invoice_number', 'payment_amount', 'payment_due_date', 'registered_name',
    'message', 'preferred_contact', 'existing_customer', 'package_selection',
    'service_type', 'moving_from_postal', 'moving_to_postal', 'move_date',
    'storage_purpose', 'seats_required', 'coworking_solution', 'tour_date',
}


# ============================================================================
# Database Connections
# ============================================================================

def get_mysql_engine(instance: str, password: str):
    """Create SQLAlchemy engine for Kinsta MySQL via SSH tunnel."""
    inst = INSTANCES[instance]
    mysql_config = DatabaseConfig(
        db_type=DatabaseType.MARIADB,
        host=inst['host'],
        port=inst['port'],
        database=inst['database'],
        username=inst['username'],
        password=password,
        pool_size=2,
        max_overflow=3,
        pool_timeout=30,
        pool_recycle=600,
    )
    return create_engine_from_config(mysql_config)


def get_pg_engine():
    """Create SQLAlchemy engine for esa_pbi PostgreSQL."""
    config = DataLayerConfig.from_env()
    pg_config = config.databases.get('postgresql')
    if not pg_config:
        raise ValueError("PostgreSQL (pbi) config not found in DataLayerConfig")
    return create_engine_from_config(pg_config)


# ============================================================================
# Dimension Extraction
# ============================================================================

def extract_dimensions(mysql_engine, pg_engine, source_instance: str):
    """Extract dimension tables from WordPress into PostgreSQL."""
    MySQLSession = sessionmaker(bind=mysql_engine)
    mysql_sess = MySQLSession()

    PGSession = sessionmaker(bind=pg_engine)
    pg_sess = PGSession()

    try:
        # --- Facilities ---
        rows = mysql_sess.execute(text(
            "SELECT ID AS post_id, post_title AS facility_name "
            "FROM wp_posts WHERE post_type = 'facility' AND post_status = 'publish'"
        )).fetchall()
        logger.info(f"Facilities: {len(rows)} rows from MySQL")

        pg_sess.execute(text(
            "DELETE FROM gf_dim_facility WHERE source_instance = :inst"
        ), {'inst': source_instance})
        for r in rows:
            pg_sess.execute(text(
                "INSERT INTO gf_dim_facility (post_id, facility_name, source_instance) "
                "VALUES (:pid, :name, :inst)"
            ), {'pid': r.post_id, 'name': r.facility_name, 'inst': source_instance})
        pg_sess.commit()
        logger.info(f"Facilities: {len(rows)} inserted into PG")

        # --- Storage Sizes (AQR) ---
        rows_aqr = mysql_sess.execute(text(
            "SELECT ID AS size_id, post_title AS size_label "
            "FROM wp_posts WHERE post_type = 'storage-size-aqr' AND post_status = 'publish'"
        )).fetchall()

        # --- Storage Sizes (legacy) ---
        rows_legacy = mysql_sess.execute(text(
            "SELECT ID AS size_id, post_title AS size_label "
            "FROM wp_posts WHERE post_type = 'storage-size' AND post_status = 'publish'"
        )).fetchall()

        logger.info(f"Storage sizes: {len(rows_aqr)} AQR + {len(rows_legacy)} legacy from MySQL")

        pg_sess.execute(text(
            "DELETE FROM gf_dim_storage_size WHERE source_instance = :inst"
        ), {'inst': source_instance})
        for r in rows_aqr:
            pg_sess.execute(text(
                "INSERT INTO gf_dim_storage_size (size_id, size_label, size_type, source_instance) "
                "VALUES (:sid, :label, 'aqr', :inst)"
            ), {'sid': r.size_id, 'label': r.size_label, 'inst': source_instance})
        for r in rows_legacy:
            pg_sess.execute(text(
                "INSERT INTO gf_dim_storage_size (size_id, size_label, size_type, source_instance) "
                "VALUES (:sid, :label, 'legacy', :inst)"
            ), {'sid': r.size_id, 'label': r.size_label, 'inst': source_instance})
        pg_sess.commit()
        logger.info(f"Storage sizes: {len(rows_aqr) + len(rows_legacy)} inserted into PG")

        # --- Storage Durations ---
        rows = mysql_sess.execute(text(
            "SELECT t.term_id AS duration_id, t.name AS duration_label "
            "FROM wp_terms t "
            "JOIN wp_term_taxonomy tt ON t.term_id = tt.term_id "
            "WHERE tt.taxonomy = 'storage-duration'"
        )).fetchall()
        logger.info(f"Storage durations: {len(rows)} rows from MySQL")

        pg_sess.execute(text(
            "DELETE FROM gf_dim_storage_duration WHERE source_instance = :inst"
        ), {'inst': source_instance})
        for r in rows:
            pg_sess.execute(text(
                "INSERT INTO gf_dim_storage_duration (duration_id, duration_label, source_instance) "
                "VALUES (:did, :label, :inst)"
            ), {'did': r.duration_id, 'label': r.duration_label, 'inst': source_instance})
        pg_sess.commit()
        logger.info(f"Storage durations: {len(rows)} inserted into PG")

    finally:
        mysql_sess.close()
        pg_sess.close()


# ============================================================================
# Form Name Lookup
# ============================================================================

def fetch_form_names(mysql_engine) -> Dict[int, str]:
    """Fetch form_id -> form_title mapping from WordPress."""
    Session = sessionmaker(bind=mysql_engine)
    sess = Session()
    try:
        rows = sess.execute(text("SELECT id, title FROM wp_gf_form")).fetchall()
        return {r.id: r.title for r in rows}
    finally:
        sess.close()


# ============================================================================
# Entry Extraction — New Schema (wp_gf_*)
# ============================================================================

def fetch_entry_ids_new(mysql_engine, form_id: int, resume_from: int = 0) -> List[int]:
    """Get all entry IDs for a form from new schema, ordered for chunking."""
    Session = sessionmaker(bind=mysql_engine)
    sess = Session()
    try:
        rows = sess.execute(text(
            "SELECT id FROM wp_gf_entry "
            "WHERE form_id = :fid AND id > :resume "
            "ORDER BY id"
        ), {'fid': form_id, 'resume': resume_from}).fetchall()
        return [r.id for r in rows]
    finally:
        sess.close()


def fetch_entries_chunk_new(mysql_engine, entry_ids: List[int]) -> List[Dict]:
    """Fetch entry header rows from wp_gf_entry for a chunk of IDs."""
    if not entry_ids:
        return []
    Session = sessionmaker(bind=mysql_engine)
    sess = Session()
    try:
        placeholders = ','.join([str(int(eid)) for eid in entry_ids])
        rows = sess.execute(text(
            f"SELECT id, form_id, date_created, date_updated, source_url, ip, "
            f"user_agent, status, payment_status, payment_date, payment_amount, "
            f"payment_method, transaction_id "
            f"FROM wp_gf_entry WHERE id IN ({placeholders})"
        )).fetchall()
        return [dict(r._mapping) for r in rows]
    finally:
        sess.close()


def fetch_meta_chunk_new(mysql_engine, entry_ids: List[int]) -> List[Dict]:
    """Fetch entry meta (EAV) from wp_gf_entry_meta for a chunk of IDs.
    Only numeric meta_keys (form field IDs)."""
    if not entry_ids:
        return []
    Session = sessionmaker(bind=mysql_engine)
    sess = Session()
    try:
        placeholders = ','.join([str(int(eid)) for eid in entry_ids])
        rows = sess.execute(text(
            f"SELECT entry_id, form_id, meta_key, meta_value "
            f"FROM wp_gf_entry_meta "
            f"WHERE entry_id IN ({placeholders}) AND meta_key REGEXP '^[0-9]+$'"
        )).fetchall()
        return [dict(r._mapping) for r in rows]
    finally:
        sess.close()


# ============================================================================
# Entry Extraction — Legacy Schema (wp_rg_*)
# ============================================================================

def fetch_legacy_entry_ids(mysql_engine, form_id: int, resume_from: int = 0) -> List[int]:
    """Get entry IDs from legacy schema NOT present in new schema."""
    Session = sessionmaker(bind=mysql_engine)
    sess = Session()
    try:
        rows = sess.execute(text(
            "SELECT l.id FROM wp_rg_lead l "
            "LEFT JOIN wp_gf_entry e ON l.id = e.id "
            "WHERE l.form_id = :fid AND e.id IS NULL AND l.id > :resume "
            "ORDER BY l.id"
        ), {'fid': form_id, 'resume': resume_from}).fetchall()
        return [r.id for r in rows]
    finally:
        sess.close()


def fetch_entries_chunk_legacy(mysql_engine, entry_ids: List[int]) -> List[Dict]:
    """Fetch entry header rows from wp_rg_lead for a chunk of IDs."""
    if not entry_ids:
        return []
    Session = sessionmaker(bind=mysql_engine)
    sess = Session()
    try:
        placeholders = ','.join([str(int(eid)) for eid in entry_ids])
        rows = sess.execute(text(
            f"SELECT id, form_id, date_created, source_url, ip, "
            f"user_agent, status, payment_status, payment_date, payment_amount, "
            f"payment_method, transaction_id "
            f"FROM wp_rg_lead WHERE id IN ({placeholders})"
        )).fetchall()
        # Adapt column names to match new schema
        results = []
        for r in rows:
            d = dict(r._mapping)
            d['date_updated'] = None  # Legacy has no date_updated
            results.append(d)
        return results
    finally:
        sess.close()


def fetch_meta_chunk_legacy(mysql_engine, entry_ids: List[int]) -> List[Dict]:
    """Fetch entry detail (EAV) from wp_rg_lead_detail for a chunk of IDs.
    field_number is DECIMAL but maps to same field IDs as meta_key."""
    if not entry_ids:
        return []
    Session = sessionmaker(bind=mysql_engine)
    sess = Session()
    try:
        placeholders = ','.join([str(int(eid)) for eid in entry_ids])
        rows = sess.execute(text(
            f"SELECT lead_id AS entry_id, form_id, "
            f"CAST(field_number AS UNSIGNED) AS meta_key, value AS meta_value "
            f"FROM wp_rg_lead_detail "
            f"WHERE lead_id IN ({placeholders})"
        )).fetchall()
        # Convert meta_key to string to match new schema convention
        results = []
        for r in rows:
            d = dict(r._mapping)
            d['meta_key'] = str(int(d['meta_key']))
            results.append(d)
        return results
    finally:
        sess.close()


# ============================================================================
# Pivot & Transform
# ============================================================================

def pivot_meta_to_entry(
    entry: Dict,
    meta_rows: List[Dict],
    field_map: Dict[str, str],
    form_name: str,
    source_instance: str,
) -> Dict:
    """
    Pivot EAV meta rows into a flat gf_entries row.

    Args:
        entry: Header row from wp_gf_entry / wp_rg_lead
        meta_rows: Meta rows for this entry
        field_map: meta_key -> column_name mapping for this form
        form_name: Form title (denormalized)
        source_instance: Instance identifier (e.g. 'kinsta_sg')
    """
    # Start with header fields
    date_utc = entry.get('date_created')
    date_sgt = None
    if date_utc:
        date_sgt = date_utc + timedelta(hours=8)

    row = {
        'entry_id': entry['id'],
        'form_id': entry['form_id'],
        'form_name': form_name,
        'status': entry.get('status'),
        'date_created_utc': date_utc,
        'date_created_sgt': date_sgt,
        'date_updated': entry.get('date_updated'),
        'source_url': entry.get('source_url'),
        'ip': entry.get('ip'),
        'user_agent': entry.get('user_agent'),
        'source_instance': source_instance,
        'synced_at': datetime.utcnow(),
    }

    # Payment fields from entry header (form 3 mainly)
    if entry.get('payment_status'):
        row['payment_status'] = entry['payment_status']
    if entry.get('payment_date'):
        row['payment_date'] = entry['payment_date']
    if entry.get('payment_method'):
        row['payment_method'] = entry['payment_method']
    if entry.get('transaction_id'):
        row['transaction_id'] = entry['transaction_id']

    # Pivot meta rows
    extra = {}
    for m in meta_rows:
        mk = str(m['meta_key'])
        mv = m.get('meta_value')
        if mv is None or mv == '':
            continue

        col = field_map.get(mk)
        if col and col in ENTRY_COLUMNS:
            # Handle ID columns — try to cast to int
            if col in ID_COLUMNS:
                try:
                    row[col] = int(mv)
                except (ValueError, TypeError):
                    row[col] = None
            elif col == 'payment_amount':
                try:
                    row[col] = float(mv)
                except (ValueError, TypeError):
                    row[col] = None
            else:
                row[col] = mv
        elif mk.isdigit():
            # Unmapped numeric field -> extra_fields
            extra[mk] = mv

    if extra:
        row['extra_fields'] = json.dumps(extra)

    return row


# ============================================================================
# PostgreSQL Insert
# ============================================================================

# All columns that can appear in gf_entries INSERT
ALL_INSERT_COLUMNS = [
    'entry_id', 'form_id', 'form_name', 'status',
    'date_created_utc', 'date_created_sgt', 'date_updated',
    'source_url', 'ip', 'user_agent',
    'salutation', 'name', 'email', 'phone', 'company',
    'facility_id', 'facility_name', 'storage_size_id', 'storage_size_label',
    'storage_duration_id', 'storage_duration_label',
    'source_channel', 'promo_code', 'unit_type',
    'utm_source', 'utm_campaign', 'utm_medium', 'utm_additional',
    'invoice_number', 'payment_amount', 'payment_due_date', 'registered_name',
    'payment_status', 'payment_date', 'payment_method', 'transaction_id',
    'message', 'preferred_contact', 'existing_customer', 'package_selection',
    'service_type', 'moving_from_postal', 'moving_to_postal', 'move_date',
    'storage_purpose', 'seats_required', 'coworking_solution', 'tour_date',
    'extra_fields', 'source_instance', 'synced_at',
]


def insert_entries_pg(pg_engine, entries: List[Dict]):
    """Batch insert pivoted entries into gf_entries with ON CONFLICT DO NOTHING."""
    if not entries:
        return
    Session = sessionmaker(bind=pg_engine)
    sess = Session()
    try:
        cols = ', '.join(ALL_INSERT_COLUMNS)
        params = ', '.join([f':{c}' for c in ALL_INSERT_COLUMNS])
        sql = (
            f"INSERT INTO gf_entries ({cols}) VALUES ({params}) "
            f"ON CONFLICT (source_instance, entry_id) DO NOTHING"
        )
        # Fill missing keys with None
        for entry in entries:
            for col in ALL_INSERT_COLUMNS:
                entry.setdefault(col, None)
        sess.execute(text(sql), entries)
        sess.commit()
    finally:
        sess.close()


def insert_meta_raw_pg(pg_engine, meta_rows: List[Dict], source_instance: str):
    """Batch insert raw EAV rows into gf_entry_meta_raw."""
    if not meta_rows:
        return
    Session = sessionmaker(bind=pg_engine)
    sess = Session()
    try:
        sql = (
            "INSERT INTO gf_entry_meta_raw "
            "(entry_id, form_id, meta_key, meta_value, source_instance, synced_at) "
            "VALUES (:entry_id, :form_id, :meta_key, :meta_value, :source_instance, :synced_at)"
        )
        now = datetime.utcnow()
        for m in meta_rows:
            m['source_instance'] = source_instance
            m['synced_at'] = now
            m.setdefault('meta_value', None)
        sess.execute(text(sql), meta_rows)
        sess.commit()
    finally:
        sess.close()


# ============================================================================
# Backfill Dimension Labels
# ============================================================================

def backfill_dim_labels(pg_engine, source_instance: str):
    """UPDATE gf_entries with resolved facility/size/duration labels from dim tables."""
    Session = sessionmaker(bind=pg_engine)
    sess = Session()
    try:
        # Facility names
        updated = sess.execute(text(
            "UPDATE gf_entries e "
            "SET facility_name = f.facility_name "
            "FROM gf_dim_facility f "
            "WHERE e.facility_id = f.post_id "
            "AND e.source_instance = f.source_instance "
            "AND e.facility_name IS NULL "
            "AND e.facility_id IS NOT NULL"
        )).rowcount
        logger.info(f"Backfill facility_name: {updated} rows")

        # Storage size labels
        updated = sess.execute(text(
            "UPDATE gf_entries e "
            "SET storage_size_label = s.size_label "
            "FROM gf_dim_storage_size s "
            "WHERE e.storage_size_id = s.size_id "
            "AND e.source_instance = s.source_instance "
            "AND e.storage_size_label IS NULL "
            "AND e.storage_size_id IS NOT NULL"
        )).rowcount
        logger.info(f"Backfill storage_size_label: {updated} rows")

        # Storage duration labels
        updated = sess.execute(text(
            "UPDATE gf_entries e "
            "SET storage_duration_label = d.duration_label "
            "FROM gf_dim_storage_duration d "
            "WHERE e.storage_duration_id = d.duration_id "
            "AND e.source_instance = d.source_instance "
            "AND e.storage_duration_label IS NULL "
            "AND e.storage_duration_id IS NOT NULL"
        )).rowcount
        logger.info(f"Backfill storage_duration_label: {updated} rows")

        sess.commit()
    finally:
        sess.close()


# ============================================================================
# Extract Pipeline (per form)
# ============================================================================

def extract_form(
    mysql_engine,
    pg_engine,
    form_id: int,
    form_name: str,
    source_instance: str,
    form_field_maps: Dict,
    resume_from: int = 0,
    schema: str = 'new',
    skip_raw: bool = False,
):
    """
    Extract a single form's entries from MySQL to PG.

    Args:
        source_instance: Instance identifier (e.g. 'kinsta_sg')
        form_field_maps: Field mappings for this instance
        schema: 'new' for wp_gf_* tables, 'legacy' for wp_rg_* tables
    """
    field_map = form_field_maps.get(form_id, {})

    # Get entry IDs
    if schema == 'new':
        entry_ids = fetch_entry_ids_new(mysql_engine, form_id, resume_from)
    else:
        entry_ids = fetch_legacy_entry_ids(mysql_engine, form_id, resume_from)

    if not entry_ids:
        logger.info(f"  Form {form_id} ({schema}): no entries to extract")
        return 0

    total = len(entry_ids)
    logger.info(f"  Form {form_id} ({schema}): {total} entries to extract")

    # Process in chunks
    inserted = 0
    chunks = [entry_ids[i:i + CHUNK_SIZE] for i in range(0, total, CHUNK_SIZE)]

    for chunk_ids in tqdm(chunks, desc=f"  Form {form_id} ({schema})", unit="chunk"):
        # Fetch entries and meta from MySQL
        if schema == 'new':
            entries = fetch_entries_chunk_new(mysql_engine, chunk_ids)
            meta_rows = fetch_meta_chunk_new(mysql_engine, chunk_ids)
        else:
            entries = fetch_entries_chunk_legacy(mysql_engine, chunk_ids)
            meta_rows = fetch_meta_chunk_legacy(mysql_engine, chunk_ids)

        # Group meta by entry_id
        meta_by_entry = {}
        for m in meta_rows:
            meta_by_entry.setdefault(m['entry_id'], []).append(m)

        # Pivot each entry
        pivoted = []
        for e in entries:
            entry_meta = meta_by_entry.get(e['id'], [])
            pivoted.append(pivot_meta_to_entry(e, entry_meta, field_map, form_name, source_instance))

        # Insert into PG
        insert_entries_pg(pg_engine, pivoted)
        if not skip_raw:
            insert_meta_raw_pg(pg_engine, meta_rows, source_instance)
        inserted += len(pivoted)

        tqdm.write(f"    Chunk done: {len(pivoted)} entries, {len(meta_rows)} meta rows "
                   f"(last entry_id: {chunk_ids[-1]})")

    return inserted


# ============================================================================
# Verify Counts
# ============================================================================

def verify_counts(mysql_engine, pg_engine, source_instance: str):
    """Compare entry counts between MySQL source and PG destination."""
    MySQLSession = sessionmaker(bind=mysql_engine)
    mysql_sess = MySQLSession()

    PGSession = sessionmaker(bind=pg_engine)
    pg_sess = PGSession()

    try:
        # New schema counts
        new_counts = mysql_sess.execute(text(
            "SELECT form_id, COUNT(*) AS cnt FROM wp_gf_entry GROUP BY form_id ORDER BY form_id"
        )).fetchall()

        # Legacy-only counts (not in new schema)
        legacy_counts = mysql_sess.execute(text(
            "SELECT l.form_id, COUNT(*) AS cnt "
            "FROM wp_rg_lead l LEFT JOIN wp_gf_entry e ON l.id = e.id "
            "WHERE e.id IS NULL "
            "GROUP BY l.form_id ORDER BY l.form_id"
        )).fetchall()

        # PG counts
        pg_counts = pg_sess.execute(text(
            "SELECT form_id, COUNT(*) AS cnt FROM gf_entries "
            "WHERE source_instance = :inst GROUP BY form_id ORDER BY form_id"
        ), {'inst': source_instance}).fetchall()

        # Build lookup
        source_totals = {}
        for r in new_counts:
            source_totals[r.form_id] = source_totals.get(r.form_id, 0) + r.cnt
        for r in legacy_counts:
            source_totals[r.form_id] = source_totals.get(r.form_id, 0) + r.cnt

        pg_totals = {r.form_id: r.cnt for r in pg_counts}

        # Report
        print(f"\n{'Form':>6} | {'MySQL':>10} | {'PG':>10} | {'Diff':>8} | Status")
        print("-" * 55)
        all_forms = sorted(set(list(source_totals.keys()) + list(pg_totals.keys())))
        total_src = 0
        total_pg = 0
        for fid in all_forms:
            src = source_totals.get(fid, 0)
            pg = pg_totals.get(fid, 0)
            diff = pg - src
            status = "OK" if diff == 0 else ("MISSING" if diff < 0 else "EXTRA")
            print(f"{fid:>6} | {src:>10,} | {pg:>10,} | {diff:>+8,} | {status}")
            total_src += src
            total_pg += pg
        print("-" * 55)
        total_diff = total_pg - total_src
        print(f"{'TOTAL':>6} | {total_src:>10,} | {total_pg:>10,} | {total_diff:>+8,} |")

        # Also verify raw meta counts
        mysql_meta = mysql_sess.execute(text(
            "SELECT COUNT(*) AS cnt FROM wp_gf_entry_meta WHERE meta_key REGEXP '^[0-9]+$'"
        )).scalar()
        legacy_meta = mysql_sess.execute(text(
            "SELECT COUNT(*) AS cnt FROM wp_rg_lead_detail d "
            "LEFT JOIN wp_gf_entry e ON d.lead_id = e.id WHERE e.id IS NULL"
        )).scalar()
        pg_meta = pg_sess.execute(text(
            "SELECT COUNT(*) FROM gf_entry_meta_raw WHERE source_instance = :inst"
        ), {'inst': source_instance}).scalar()
        print(f"\nMeta rows: MySQL new={mysql_meta:,} + legacy={legacy_meta:,} "
              f"= {mysql_meta + legacy_meta:,} | PG={pg_meta:,}")

    finally:
        mysql_sess.close()
        pg_sess.close()


# ============================================================================
# CLI
# ============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description='Gravity Forms WordPress to PostgreSQL Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python gravityforms_to_sql.py --instance sg --mode dimensions
  python gravityforms_to_sql.py --instance sg --mode full
  python gravityforms_to_sql.py --instance all --mode full
  python gravityforms_to_sql.py --instance hk --mode full --form-id 8
  python gravityforms_to_sql.py --instance sg --mode full --resume-from 150000
  python gravityforms_to_sql.py --instance sg --mode verify
        """
    )
    parser.add_argument('--instance', choices=['sg', 'my', 'kr', 'hk', 'all'],
                        required=True, help='Kinsta instance (or "all" for all instances)')
    parser.add_argument('--mode', choices=['dimensions', 'full', 'verify'],
                        required=True, help='Extraction mode')
    parser.add_argument('--form-id', type=int, default=None,
                        help='Extract single form only')
    parser.add_argument('--resume-from', type=int, default=0,
                        help='Resume from entry_id (for tunnel drops)')
    parser.add_argument('--mysql-password', type=str, default=None,
                        help='MySQL password (overrides built-in, applies to single instance)')
    parser.add_argument('--skip-legacy', action='store_true',
                        help='Skip legacy wp_rg_* tables')
    parser.add_argument('--only-legacy', action='store_true',
                        help='Only extract legacy wp_rg_* tables (skip new schema)')
    parser.add_argument('--skip-raw', action='store_true',
                        help='Skip inserting raw meta rows (saves time on re-runs)')
    return parser.parse_args()


def get_mysql_password(instance: str, override: Optional[str] = None) -> str:
    """Resolve MySQL password for an instance."""
    if override:
        return override
    import os
    env_key = INSTANCES[instance]['password_env']
    env_val = os.environ.get(env_key)
    if env_val:
        return env_val
    # Fall back to built-in passwords
    pw = MYSQL_PASSWORDS.get(instance)
    if pw:
        return pw
    import getpass
    return getpass.getpass(f"MySQL password for {instance}: ")


def run_instance(instance: str, args, pg_engine):
    """Run extraction for a single instance."""
    inst_config = INSTANCES[instance]
    source_instance = inst_config['source_instance']
    form_field_maps = INSTANCE_FORM_FIELD_MAPS[instance]
    mysql_password = get_mysql_password(instance, args.mysql_password)

    print("=" * 70)
    print(f"Instance: {instance.upper()} ({source_instance})")
    print(f"Mode: {args.mode.upper()}")
    print(f"Source: MySQL {inst_config['host']}:{inst_config['port']}/{inst_config['database']}")
    print(f"Target: PostgreSQL (esa_pbi)")
    if args.form_id:
        print(f"Form filter: {args.form_id}")
    if args.resume_from:
        print(f"Resume from entry_id: {args.resume_from}")
    print("=" * 70)

    mysql_engine = get_mysql_engine(instance, mysql_password)

    if args.mode == 'dimensions':
        extract_dimensions(mysql_engine, pg_engine, source_instance)
        print(f"\n{instance.upper()} dimensions extracted successfully.")

    elif args.mode == 'full':
        # Extract dimensions first
        print("\n--- Extracting dimensions ---")
        extract_dimensions(mysql_engine, pg_engine, source_instance)

        # Fetch form names
        form_names = fetch_form_names(mysql_engine)

        # Determine which forms to extract
        if args.form_id:
            form_ids = [args.form_id]
        else:
            form_ids = sorted(form_field_maps.keys())

        total_inserted = 0

        # New schema entries
        if not getattr(args, 'only_legacy', False):
            print("\n--- Extracting new schema entries (wp_gf_*) ---")
        for fid in form_ids if not getattr(args, 'only_legacy', False) else []:
            fname = form_names.get(fid, f'Form {fid}')
            print(f"\nForm {fid}: {fname}")
            count = extract_form(
                mysql_engine, pg_engine, fid, fname,
                source_instance=source_instance,
                form_field_maps=form_field_maps,
                resume_from=args.resume_from, schema='new',
                skip_raw=args.skip_raw,
            )
            total_inserted += count

        # Legacy schema entries
        if not args.skip_legacy:
            print("\n--- Extracting legacy entries (wp_rg_*) ---")
            for fid in form_ids:
                fname = form_names.get(fid, f'Form {fid}')
                count = extract_form(
                    mysql_engine, pg_engine, fid, fname,
                    source_instance=source_instance,
                    form_field_maps=form_field_maps,
                    resume_from=args.resume_from, schema='legacy',
                    skip_raw=args.skip_raw,
                )
                if count > 0:
                    print(f"  Form {fid} legacy: {count} entries")
                total_inserted += count

        # Backfill dimension labels
        print("\n--- Backfilling dimension labels ---")
        backfill_dim_labels(pg_engine, source_instance)

        print(f"\n{instance.upper()} total entries inserted: {total_inserted:,}")

    elif args.mode == 'verify':
        verify_counts(mysql_engine, pg_engine, source_instance)


def main():
    args = parse_args()

    # PG engine (shared across instances)
    pg_engine = get_pg_engine()

    if args.instance == 'all':
        for instance in ['sg', 'my', 'kr', 'hk']:
            try:
                run_instance(instance, args, pg_engine)
            except Exception as e:
                logger.error(f"Failed on instance {instance}: {e}")
                print(f"\nERROR on {instance}: {e}")
                continue
            print()
    else:
        run_instance(args.instance, args, pg_engine)

    print("\nDone.")


if __name__ == "__main__":
    main()
