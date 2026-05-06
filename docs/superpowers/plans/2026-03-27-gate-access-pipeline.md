# Gate Access Data Pipeline & UI Integration

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fetch gate access data from SMD SOAP API, store encrypted in esa_backend DB, and display gate lock/overlock status + masked access codes on the smart lock assignments page.

**Architecture:** New datalayer pipeline calls `GateAccessData` SOAP operation per site, encrypts access codes with Fernet (VAULT_MASTER_KEY + dedicated salt via PBKDF2), and upserts to `gate_access_data` table in esa_backend. The assignments page merges this data, showing status badges and masked codes with a reveal button that decrypts on demand via a new API endpoint. Pipeline runs nightly at 2am + manual refresh from the UI.

**Tech Stack:** Python, Flask, SQLAlchemy, SOAPClient, Fernet/PBKDF2 encryption, APScheduler, vanilla JS

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `backend/python/web/models/smart_lock.py` | Modify | Add `GateAccessData` model |
| `backend/python/datalayer/gate_access_to_sql.py` | Create | Pipeline: SOAP fetch → encrypt → upsert |
| `backend/python/common/gate_access_crypto.py` | Create | Encrypt/decrypt helpers using VAULT_MASTER_KEY + salt |
| `backend/python/migrations/031_gate_access_data.sql` | Create | Table DDL |
| `backend/python/config/pipelines.yaml` | Modify | Register `gateaccess` pipeline (2am daily) |
| `backend/config/scheduler.yaml` | Modify | Add `gateaccess` pipeline location_codes |
| `backend/python/web/routes/api.py` | Modify | Add reveal endpoint + merge gate data into `/api/smart-lock/units` |
| `backend/python/web/templates/tools/smart_lock_assignments.html` | Modify | Gate columns, masked codes, reveal button, refresh button |

---

### Task 1: Migration — Create `gate_access_data` Table

**Files:**
- Create: `backend/python/migrations/031_gate_access_data.sql`

- [ ] **Step 1: Write the migration SQL**

```sql
-- Gate Access Data from SMD GateAccessData SOAP endpoint
-- Stores per-unit gate access info with encrypted access codes
-- Target DB: esa_backend

CREATE TABLE IF NOT EXISTS gate_access_data (
    id              SERIAL PRIMARY KEY,
    location_code   VARCHAR(10)  NOT NULL,          -- sLocationCode (L001, L002, etc.)
    site_id         INTEGER      NOT NULL,          -- Numeric SiteID (48, 49, etc.)
    unit_id         INTEGER      NOT NULL,          -- StorageMaker UnitID
    unit_name       VARCHAR(50)  NOT NULL,          -- sUnitName
    is_rented       BOOLEAN      NOT NULL DEFAULT false,
    access_code_enc TEXT,                            -- Fernet-encrypted sAccessCode
    access_code2_enc TEXT,                           -- Fernet-encrypted sAccessCode2
    is_gate_locked  BOOLEAN      NOT NULL DEFAULT false,
    is_overlocked   BOOLEAN      NOT NULL DEFAULT false,
    keypad_zone     INTEGER      NOT NULL DEFAULT 0,
    updated_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    created_at      TIMESTAMP    NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_gate_access_loc_unit UNIQUE (location_code, unit_id)
);

CREATE INDEX IF NOT EXISTS ix_gate_access_location_code ON gate_access_data (location_code);
CREATE INDEX IF NOT EXISTS ix_gate_access_site_id ON gate_access_data (site_id);
CREATE INDEX IF NOT EXISTS ix_gate_access_unit_name ON gate_access_data (location_code, unit_name);
```

- [ ] **Step 2: Run the migration against esa_backend**

```bash
DB_PW=$(python3 -c "from dotenv import load_dotenv; load_dotenv('.env'); import os; print(os.environ['DB_PASSWORD'])")
PGPASSWORD="$DB_PW" psql "host=esapbi.postgres.database.azure.com port=5432 dbname=backend user=esa_pbi_admin sslmode=require" \
  -f backend/python/migrations/031_gate_access_data.sql
```

Expected: `CREATE TABLE`, `CREATE INDEX` x2

- [ ] **Step 3: Commit**

```bash
git add backend/python/migrations/031_gate_access_data.sql
git commit -m "feat(smart-lock): add gate_access_data table migration"
```

---

### Task 2: Encryption Helper — `gate_access_crypto.py`

**Files:**
- Create: `backend/python/common/gate_access_crypto.py`

- [ ] **Step 1: Create the crypto helper module**

This module derives a Fernet key from `VAULT_MASTER_KEY` + a dedicated salt stored in `app_secrets` as `_GATE_ACCESS_SALT`. Same PBKDF2 pattern as `db_secrets_vault.py`.

```python
"""
Gate access code encryption/decryption.

Uses VAULT_MASTER_KEY + a dedicated PBKDF2 salt (_GATE_ACCESS_SALT in app_secrets)
to derive a Fernet key. Access codes are encrypted before DB storage and decrypted
on demand when a user clicks "reveal" in the UI.
"""

import base64
import logging
import os
import secrets as secrets_module
import threading

from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

_SALT_KEY = '_GATE_ACCESS_SALT'
_ITERATIONS = 100_000

# Module-level singleton
_instance = None
_lock = threading.Lock()


class GateAccessCrypto:
    """Encrypt/decrypt gate access codes using VAULT_MASTER_KEY + dedicated salt."""

    def __init__(self, master_key: str, db_url):
        self._engine = create_engine(db_url, pool_pre_ping=True)
        self._session_factory = sessionmaker(bind=self._engine)
        salt = self._load_or_create_salt()
        self._fernet = self._derive_fernet(master_key, salt)

    def _derive_fernet(self, master_key: str, salt: bytes) -> Fernet:
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=_ITERATIONS,
            backend=default_backend(),
        )
        key = base64.urlsafe_b64encode(kdf.derive(master_key.encode()))
        return Fernet(key)

    def _load_or_create_salt(self) -> bytes:
        from common.db_secrets_vault import AppSecretRow  # same model used by vault

        session = self._session_factory()
        try:
            row = session.query(AppSecretRow).filter(
                AppSecretRow.key == _SALT_KEY
            ).first()

            if row:
                return base64.b64decode(row.value_encrypted)

            # First run — generate 16-byte random salt, store raw (not Fernet-encrypted)
            salt = secrets_module.token_bytes(16)
            meta = AppSecretRow(
                key=_SALT_KEY,
                value_encrypted=base64.b64encode(salt).decode('utf-8'),
                environment='all',
                description='PBKDF2 salt for gate access code encryption (do not delete)',
                updated_by='system',
            )
            session.add(meta)
            session.commit()
            logger.info("Generated new gate access encryption salt")
            return salt
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def encrypt(self, plaintext: str) -> str:
        """Encrypt an access code. Returns base64 Fernet ciphertext."""
        if not plaintext:
            return ''
        return self._fernet.encrypt(plaintext.encode('utf-8')).decode('utf-8')

    def decrypt(self, ciphertext: str) -> str:
        """Decrypt an access code. Returns plaintext."""
        if not ciphertext:
            return ''
        return self._fernet.decrypt(ciphertext.encode('utf-8')).decode('utf-8')


def get_gate_crypto() -> GateAccessCrypto:
    """Get or create the module-level singleton."""
    global _instance
    if _instance is None:
        with _lock:
            if _instance is None:
                from common.config_loader import get_database_url
                master_key = os.environ.get('VAULT_MASTER_KEY')
                if not master_key:
                    raise RuntimeError("VAULT_MASTER_KEY not set")
                db_url = get_database_url('backend')
                _instance = GateAccessCrypto(master_key, db_url)
    return _instance
```

- [ ] **Step 2: Commit**

```bash
git add backend/python/common/gate_access_crypto.py
git commit -m "feat(smart-lock): add gate access code encryption helper"
```

---

### Task 3: SQLAlchemy Model — `GateAccessData`

**Files:**
- Modify: `backend/python/web/models/smart_lock.py`

- [ ] **Step 1: Add the GateAccessData model**

Append to `backend/python/web/models/smart_lock.py`:

```python
class GateAccessData(Base):
    """Gate access data from SMD GateAccessData SOAP endpoint.
    Access codes are Fernet-encrypted at rest."""
    __tablename__ = 'gate_access_data'

    id = Column(Integer, primary_key=True)
    location_code = Column(String(10), nullable=False)
    site_id = Column(Integer, nullable=False)
    unit_id = Column(Integer, nullable=False)
    unit_name = Column(String(50), nullable=False)
    is_rented = Column(Boolean, nullable=False, default=False)
    access_code_enc = Column(Text)
    access_code2_enc = Column(Text)
    is_gate_locked = Column(Boolean, nullable=False, default=False)
    is_overlocked = Column(Boolean, nullable=False, default=False)
    keypad_zone = Column(Integer, nullable=False, default=0)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('location_code', 'unit_id', name='uq_gate_access_loc_unit'),
    )

    def to_dict(self):
        return {
            'location_code': self.location_code,
            'site_id': self.site_id,
            'unit_id': self.unit_id,
            'unit_name': self.unit_name,
            'is_rented': self.is_rented,
            'is_gate_locked': self.is_gate_locked,
            'is_overlocked': self.is_overlocked,
            'keypad_zone': self.keypad_zone,
            'has_access_code': bool(self.access_code_enc),
            'has_access_code2': bool(self.access_code2_enc),
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }
```

Add `Boolean, Text` to the SQLAlchemy imports at the top of the file.

- [ ] **Step 2: Commit**

```bash
git add backend/python/web/models/smart_lock.py
git commit -m "feat(smart-lock): add GateAccessData model"
```

---

### Task 4: Pipeline — `gate_access_to_sql.py`

**Files:**
- Create: `backend/python/datalayer/gate_access_to_sql.py`

- [ ] **Step 1: Create the pipeline script**

Follow the `units_info_to_sql.py` pattern exactly. Key differences:
- SOAP operation: `GateAccessData` (not `UnitsInformation_v3`)
- SOAPAction: `http://tempuri.org/CallCenterWs/CallCenterWs/GateAccessData`
- Extra param: `iMinutesSinceLastUpdate=0`
- Result tag: `Table` (same as units_info)
- Encrypt `sAccessCode` and `sAccessCode2` before storage
- Target DB: esa_backend (use `get_database_url('backend')`)
- Upsert on: `(site_id, unit_id)`

```python
"""
GateAccessData to SQL Pipeline

Fetches gate access data from GateAccessData SOAP API (CallCenterWs)
and pushes to PostgreSQL (esa_backend) with encrypted access codes.

Features:
- Fetches all unit gate access records for configured locations
- Encrypts sAccessCode/sAccessCode2 with Fernet (VAULT_MASTER_KEY + dedicated salt)
- Uses upsert on composite key (site_id + unit_id)
- Processes in chunks for large datasets

Usage:
    python gate_access_to_sql.py

Configuration (in pipelines.yaml):
    pipelines.gateaccess.location_codes: shared location_codes
    pipelines.gateaccess.sql_chunk_size: Batch size for upsert (default: 500)
"""

import logging
import sys
from pathlib import Path
from typing import List, Dict, Any

from tqdm import tqdm

logger = logging.getLogger(__name__)

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from common import (
    DataLayerConfig,
    SOAPClient,
    SessionManager,
    UpsertOperations,
    convert_to_bool,
    convert_to_int,
    deduplicate_records,
)
from common.config import get_pipeline_config
from common.gate_access_crypto import get_gate_crypto
from common.config_loader import get_database_url
from sqlalchemy import create_engine

# Import model — use web.models path since GateAccessData is in esa_backend
# NOTE: This is the first datalayer script importing from web.models (intentional —
# this pipeline targets esa_backend, not esa_pbi like other pipelines)
from web.models.smart_lock import GateAccessData

# =============================================================================
# SOAP Configuration
# =============================================================================
CALL_CENTER_WS_URL = "https://api.smdservers.net/CCWs_3.5/CallCenterWs.asmx"
NAMESPACE = "http://tempuri.org/CallCenterWs/CallCenterWs"
SOAP_ACTION = "http://tempuri.org/CallCenterWs/CallCenterWs/GateAccessData"


# =============================================================================
# Record Transformation
# =============================================================================

def transform_record(
    record: Dict[str, Any],
    location_code: str,
    site_id: int,
    crypto: 'GateAccessCrypto'
) -> Dict[str, Any]:
    """Transform SOAP record to DB-ready format with encrypted access codes."""
    access_code = record.get('sAccessCode') or ''
    access_code2 = record.get('sAccessCode2') or ''

    return {
        'location_code': location_code,
        'site_id': site_id,
        'unit_id': convert_to_int(record.get('UnitID')),
        'unit_name': record.get('sUnitName') or '',
        'is_rented': convert_to_bool(record.get('bRented')),
        'access_code_enc': crypto.encrypt(access_code) if access_code else None,
        'access_code2_enc': crypto.encrypt(access_code2) if access_code2 else None,
        'is_gate_locked': convert_to_bool(record.get('bGateLocked')),
        'is_overlocked': convert_to_bool(record.get('bOverlocked')),
        'keypad_zone': convert_to_int(record.get('iKeypadZ')) or 0,
    }


# =============================================================================
# Data Operations
# =============================================================================

def build_location_to_site_map() -> Dict[str, int]:
    """Build location_code → numeric SiteID mapping from esa_pbi units_info."""
    from common.config_loader import get_database_url
    pbi_engine = create_engine(get_database_url('pbi'))
    from sqlalchemy import text
    with pbi_engine.connect() as conn:
        rows = conn.execute(text(
            'SELECT DISTINCT "sLocationCode", "SiteID" FROM units_info '
            'WHERE "sLocationCode" IS NOT NULL'
        )).fetchall()
    return {r[0]: r[1] for r in rows}


def fetch_gate_access(
    soap_client: SOAPClient,
    location_codes: List[str],
    crypto: 'GateAccessCrypto',
    loc_to_site: Dict[str, int],
) -> List[Dict[str, Any]]:
    """Fetch and transform gate access data for all locations."""
    all_data = []

    with tqdm(total=len(location_codes), desc="  Fetching locations", unit="loc") as pbar:
        for location_code in location_codes:
            site_id = loc_to_site.get(location_code)
            if site_id is None:
                tqdm.write(f"  ⚠ {location_code}: no SiteID mapping found, skipping")
                pbar.update(1)
                continue

            try:
                results = soap_client.call(
                    operation="GateAccessData",
                    parameters={
                        "sLocationCode": location_code.strip(),
                        "iMinutesSinceLastUpdate": "0",
                    },
                    soap_action=SOAP_ACTION,
                    namespace=NAMESPACE,
                    result_tag="Table",
                )

                for record in results:
                    transformed = transform_record(record, location_code, site_id, crypto)
                    if transformed['unit_id']:  # skip records with no UnitID
                        all_data.append(transformed)

                pbar.set_postfix({"location": location_code, "units": len(results)})
                pbar.update(1)

            except Exception as e:
                logger.debug("SOAP fetch failed for %s", location_code, exc_info=True)
                pbar.set_postfix({"location": location_code, "status": "ERROR"})
                pbar.update(1)
                tqdm.write(f"  ✗ {location_code}: SOAP fetch failed")
                continue

    original_count = len(all_data)
    all_data = deduplicate_records(all_data, ['location_code', 'unit_id'])
    if len(all_data) < original_count:
        tqdm.write(f"  ℹ Deduplicated: {original_count} → {len(all_data)} records")

    return all_data


def push_to_database(
    data: List[Dict[str, Any]],
    chunk_size: int = 500,
) -> None:
    """Push gate access data to esa_backend PostgreSQL."""
    if not data:
        print("  ⚠ No data to push")
        return

    db_url = get_database_url('backend')
    engine = create_engine(db_url)
    # Table created by migration 031_gate_access_data.sql — no create_all needed

    session_manager = SessionManager(engine)
    num_chunks = (len(data) + chunk_size - 1) // chunk_size

    with session_manager.session_scope() as session:
        upsert_ops = UpsertOperations(session, 'postgresql')

        with tqdm(total=len(data), desc="  Upserting records", unit="rec") as pbar:
            for i in range(0, len(data), chunk_size):
                chunk = data[i:i + chunk_size]

                upsert_ops.upsert_batch(
                    model=GateAccessData,
                    records=chunk,
                    constraint_columns=['location_code', 'unit_id'],
                    chunk_size=chunk_size,
                )

                pbar.update(len(chunk))
                pbar.set_postfix({"chunk": f"{i // chunk_size + 1}/{num_chunks}"})

    tqdm.write(f"  ✓ Upserted {len(data)} records to esa_backend")


# =============================================================================
# Main
# =============================================================================

def main():
    """Main pipeline function."""
    config = DataLayerConfig.from_env()

    if not config.soap:
        raise ValueError("SOAP configuration not found. Check apis.yaml and vault secrets.")

    location_codes = get_pipeline_config('gateaccess', 'location_codes', [])
    if not location_codes:
        # Fall back to shared location_codes
        location_codes = get_pipeline_config('unitsinfo', 'location_codes', [])
    if not location_codes:
        raise ValueError("No location_codes configured")

    chunk_size = get_pipeline_config('gateaccess', 'sql_chunk_size', 500)

    soap_client = SOAPClient(
        base_url=CALL_CENTER_WS_URL,
        corp_code=config.soap.corp_code,
        corp_user=config.soap.corp_user,
        api_key=config.soap.api_key,
        corp_password=config.soap.corp_password,
        timeout=120,
        retries=3,
    )

    crypto = get_gate_crypto()

    # Build location_code → numeric SiteID mapping from esa_pbi
    loc_to_site = build_location_to_site_map()

    print("=" * 70)
    print("GateAccessData to SQL Pipeline")
    print("=" * 70)
    print(f"Endpoint: CallCenterWs/GateAccessData")
    print(f"Locations: {len(location_codes)} ({', '.join(location_codes[:5])}...)")
    print(f"Site mappings: {len(loc_to_site)} location codes resolved")
    print(f"Target: esa_backend (PostgreSQL)")
    print("=" * 70)
    print("[STAGE:INIT] GateAccessData")

    print("[STAGE:FETCH] Retrieving gate access from SOAP API")
    all_data = fetch_gate_access(soap_client, location_codes, crypto, loc_to_site)

    if all_data:
        print("[STAGE:PUSH] Upserting to PostgreSQL")
        push_to_database(all_data, chunk_size)

        # Summary
        print("\n[Summary]")
        print("-" * 70)
        locked = sum(1 for r in all_data if r.get('is_gate_locked'))
        overlocked = sum(1 for r in all_data if r.get('is_overlocked'))
        with_code = sum(1 for r in all_data if r.get('access_code_enc'))
        print(f"  Total records: {len(all_data)}")
        print(f"  Gate locked: {locked}")
        print(f"  Overlocked: {overlocked}")
        print(f"  With access code: {with_code}")
    else:
        print("\n⚠ No data found for any location")

    soap_client.close()

    print(f"[STAGE:COMPLETE] {len(all_data)} records")
    print("=" * 70)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add backend/python/datalayer/gate_access_to_sql.py
git commit -m "feat(smart-lock): add gate access data pipeline"
```

---

### Task 5: Register Pipeline in Scheduler Config

**Files:**
- Modify: `backend/python/config/pipelines.yaml`
- Modify: `backend/config/scheduler.yaml`

- [ ] **Step 1: Add `gateaccess` pipeline entry to `pipelines.yaml`**

Append to the `pipelines:` section in `pipelines.yaml`:

```yaml
  gateaccess:
    display_name: Gate Access Data
    description: Fetch gate access data (lock codes, lock status) from SOAP API
    module_path: datalayer.gate_access_to_sql
    enabled: true
    schedule:
      type: cron
      cron: 0 2 * * *
    priority: 8
    resource_group: soap_api
    max_db_connections: 2
    estimated_duration_seconds: 300
    retry:
      max_attempts: 3
      delay_seconds: 300
      backoff_multiplier: 2
    timeout_seconds: 1200
    data_freshness:
      table: gate_access_data
      date_column: updated_at
      database: backend
```

- [ ] **Step 2: Add `gateaccess` to `scheduler.yaml`**

In `backend/config/scheduler.yaml`, add under the `pipelines:` section (alongside rentroll, discount, etc.):

```yaml
  gateaccess:
    sql_chunk_size: 500
    location_codes: *location_codes
```

- [ ] **Step 3: Commit**

```bash
git add backend/python/config/pipelines.yaml backend/config/scheduler.yaml
git commit -m "feat(smart-lock): register gate access pipeline (2am daily)"
```

---

### Task 6: API — Merge Gate Data into Units Endpoint + Reveal Endpoint

**Files:**
- Modify: `backend/python/web/routes/api.py`

- [ ] **Step 1: Merge gate access data into `GET /api/smart-lock/units`**

In the `api_sl_units()` function (around line 4290), after fetching assignments from esa_backend, also query `gate_access_data`:

```python
# After the existing assignment query, add:
gate_data = session.query(GateAccessData).filter(
    GateAccessData.site_id.in_(site_ids)
).all()
gate_map = {g.unit_id: g.to_dict() for g in gate_data}
# UnitIDs are unique across sites (verified), so unit_id alone is sufficient

# In the unit merge loop, add:
for u in units:
    key = (u['SiteID'], u['UnitID'])
    u['assignment'] = assign_map.get(key)
    u['gate_access'] = gate_map.get(u['UnitID'])
```

> **Note:** `GateAccessData.site_id` is now the numeric SiteID (same as units_info), so the filter `site_id.in_(site_ids)` works directly. UnitIDs are unique across sites, so `gate_map[unit_id]` is unambiguous.

Also add the gate data last refresh timestamp to the response.

- [ ] **Step 2: Add `GET /api/smart-lock/gate-code` reveal endpoint**

Add a new endpoint near the other smart-lock routes:

```python
@api_bp.route('/smart-lock/gate-code')
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def api_sl_gate_code():
    """Decrypt and return a single unit's gate access code."""
    unit_id = request.args.get('unit_id', type=int)
    location_code = request.args.get('location_code', '')

    if not unit_id or not location_code:
        return jsonify({'error': 'unit_id and location_code required'}), 400

    session = get_session()
    try:
        record = session.query(GateAccessData).filter_by(
            location_code=location_code, unit_id=unit_id
        ).first()

        if not record:
            return jsonify({'error': 'No gate access data for this unit'}), 404

        from common.gate_access_crypto import get_gate_crypto
        crypto = get_gate_crypto()

        code1 = crypto.decrypt(record.access_code_enc) if record.access_code_enc else ''
        code2 = crypto.decrypt(record.access_code2_enc) if record.access_code2_enc else ''

        # Audit the reveal
        _sl_audit(
            session, 'gate_code_viewed', 'gate_access', str(unit_id),
            site_id=location_code, unit_id=unit_id,
            detail=f"Access code revealed for unit {record.unit_name}",
        )

        return jsonify({
            'access_code': code1,
            'access_code2': code2,
        })
    except Exception as e:
        logger.error("Gate code reveal failed: %s", e)
        return jsonify({'error': 'Failed to decrypt access code'}), 500
    finally:
        session.close()
```

- [ ] **Step 3: Add GateAccessData import at the top of api.py**

```python
from web.models.smart_lock import (
    SmartLockKeypad, SmartLockPadlock, SmartLockUnitAssignment,
    SmartLockAuditLog, GateAccessData,
)
```

- [ ] **Step 4: Commit**

```bash
git add backend/python/web/routes/api.py
git commit -m "feat(smart-lock): add gate access data to units endpoint + reveal API"
```

---

### Task 7: UI — Gate Access Columns on Assignments Page

**Files:**
- Modify: `backend/python/web/templates/tools/smart_lock_assignments.html`

- [ ] **Step 1: Add gate access columns to the table header**

In the `<thead>` section, after the existing columns (Site, Unit ID, Unit Name, Rentable, Rented, Keypad, Padlock), add:

```html
<th>Gate</th>
<th>Overlock</th>
<th>Access Code</th>
```

- [ ] **Step 2: Add gate access cells to `renderTable()`**

In the row rendering loop, add cells for each unit:

```javascript
// Gate locked badge
const gateLocked = u.gate_access && u.gate_access.is_gate_locked;
const gateCell = `<td>${gateLocked
    ? '<span class="badge bg-danger">Locked</span>'
    : '<span class="badge bg-success">OK</span>'}</td>`;

// Overlock badge
const overlocked = u.gate_access && u.gate_access.is_overlocked;
const overlockCell = `<td>${overlocked
    ? '<span class="badge bg-warning text-dark">Overlocked</span>'
    : ''}</td>`;

// Access code — masked with reveal button
const hasCode = u.gate_access && u.gate_access.has_access_code;
const codeCell = `<td>${hasCode
    ? `<span class="gate-code-masked" id="gc-${u.UnitID}">••••••••</span>
       <button class="btn btn-sm btn-outline-secondary ms-1 gate-reveal-btn"
               onclick="revealGateCode('${u.gate_access.location_code}', ${u.UnitID}, this)"
               title="Reveal access code">
           <i class="bi bi-eye"></i>
       </button>`
    : '<span class="text-muted">—</span>'}</td>`;
```

- [ ] **Step 3: Add the `revealGateCode()` JavaScript function**

```javascript
async function revealGateCode(siteId, unitId, btn) {
    const span = document.getElementById(`gc-${unitId}`);
    if (!span) return;

    // If already revealed, toggle back to masked
    if (span.dataset.revealed === 'true') {
        span.textContent = '••••••••';
        span.dataset.revealed = 'false';
        btn.innerHTML = '<i class="bi bi-eye"></i>';
        return;
    }

    btn.disabled = true;
    btn.innerHTML = '<i class="bi bi-hourglass-split"></i>';

    try {
        const res = await fetch(
            `/api/smart-lock/gate-code?location_code=${encodeURIComponent(siteId)}&unit_id=${unitId}`,
            { headers: apiHeaders() }
        );
        if (!res.ok) throw new Error('Failed to fetch');
        const data = await res.json();

        let display = data.access_code || '(empty)';
        if (data.access_code2) display += ` / ${data.access_code2}`;
        span.textContent = display;
        span.dataset.revealed = 'true';
        btn.innerHTML = '<i class="bi bi-eye-slash"></i>';

        // Auto-hide after 10 seconds
        setTimeout(() => {
            span.textContent = '••••••••';
            span.dataset.revealed = 'false';
            btn.innerHTML = '<i class="bi bi-eye"></i>';
        }, 10000);
    } catch (e) {
        span.textContent = 'Error';
        setTimeout(() => { span.textContent = '••••••••'; }, 3000);
    } finally {
        btn.disabled = false;
    }
}
```

- [ ] **Step 4: Add "Refresh Gate Data" button next to existing "Refresh Unit Data" button**

```javascript
async function refreshGateData() {
    const btn = document.getElementById('refresh-gate-btn');
    btn.disabled = true;
    btn.innerHTML = '<i class="bi bi-hourglass-split"></i> Refreshing...';

    try {
        const res = await fetch('/api/jobs/gateaccess/run-async', {
            method: 'POST',
            headers: apiHeaders(),
        });
        const data = await res.json();
        const execId = data.execution_id;

        // Poll for completion
        const poll = async () => {
            const r = await fetch(`/api/history/${execId}`, { headers: apiHeaders() });
            const d = await r.json();
            if (d.status === 'completed') {
                btn.innerHTML = '<i class="bi bi-shield-lock"></i> Refresh Gate Data';
                btn.disabled = false;
                showStatus('Gate access data refreshed', 'success');
                if (loadedSiteIds && loadedSiteIds.length) loadUnits(loadedSiteIds);
            } else if (d.status === 'failed') {
                btn.innerHTML = '<i class="bi bi-shield-lock"></i> Refresh Gate Data';
                btn.disabled = false;
                showStatus('Gate data refresh failed', 'danger');
            } else {
                setTimeout(poll, 5000);
            }
        };
        setTimeout(poll, 5000);
    } catch (e) {
        btn.innerHTML = '<i class="bi bi-shield-lock"></i> Refresh Gate Data';
        btn.disabled = false;
        showStatus('Failed to trigger refresh', 'danger');
    }
}
```

Add the button HTML near the existing refresh button:

```html
<button id="refresh-gate-btn" class="btn btn-outline-warning btn-sm"
        onclick="refreshGateData()" style="display:none">
    <i class="bi bi-shield-lock"></i> Refresh Gate Data
</button>
```

Show it when units are loaded (same as the other action buttons).

- [ ] **Step 5: Add gate data timestamp to the status area**

Display when gate access data was last refreshed (from the `updated_at` of gate_access_data records).

- [ ] **Step 6: Commit**

```bash
git add backend/python/web/templates/tools/smart_lock_assignments.html
git commit -m "feat(smart-lock): add gate access columns with masked codes and reveal button"
```

---

### Task 8: (Removed — no longer needed)

The `gate_access_data` table now has both `location_code` and `site_id`. The frontend gets `location_code` from `u.gate_access.location_code` (returned by `to_dict()`), so no changes to the units_info query are needed.

---

### Task 9: Deploy & Test

- [ ] **Step 1: Deploy to VM**

```bash
python scripts/deploy_to_vm.py
```

- [ ] **Step 2: Run the migration on esa_backend**

(Already covered in Task 1 step 2, but verify on VM)

- [ ] **Step 3: Trigger the pipeline manually via the scheduler UI or API**

```bash
curl -X POST https://your-domain/api/jobs/gateaccess/run-async \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json"
```

- [ ] **Step 4: Verify data in the table**

```sql
SELECT location_code, site_id, COUNT(*),
       COUNT(*) FILTER (WHERE is_gate_locked) as locked,
       COUNT(*) FILTER (WHERE access_code_enc IS NOT NULL) as has_code
FROM gate_access_data
GROUP BY location_code, site_id
ORDER BY location_code;
```

- [ ] **Step 5: Test the UI**

- Load units on assignments page
- Verify Gate/Overlock columns appear
- Click reveal button → access code shows for 10 seconds
- Click refresh gate data → pipeline runs, data updates

---

## Key Design Decisions

1. **esa_backend DB** (not esa_pbi) — this is app tool data, not analytics
2. **VAULT_MASTER_KEY + dedicated salt** — reuses existing vault infrastructure, separate salt for domain isolation
3. **Encrypt at pipeline time** — plaintext never hits the DB
4. **Decrypt on demand** — single-unit reveal endpoint, no bulk decrypt
5. **Audit on reveal** — `gate_code_viewed` action logged to SmartLockAuditLog
6. **10s auto-hide** — plaintext doesn't linger in the browser
7. **GateAccessData doesn't return all units** (~90-95% coverage) — the UI gracefully handles missing gate data with "—"
