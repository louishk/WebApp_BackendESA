# MCP Table-Level Access Control — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add per-API-key table restrictions within MCP database presets, so different users connecting to the same DB see/query only their allowed tables.

**Architecture:** New JSONB column `mcp_db_table_rules` on `api_keys` stores `{"preset_name": ["table1", "table2"]}`. Empty dict = no restrictions. Preset key with list = whitelist only those tables. Enforcement at 3 layers: `DB_list_tables` (filter output), `DB_describe_table` (block access), `DB_execute_query` (regex table extraction). Admin UI fetches live table lists via AJAX and renders dynamic checkboxes per preset.

**Tech Stack:** Python/Flask, SQLAlchemy, PostgreSQL JSONB, Starlette (MCP server), Jinja2 + vanilla JS (admin UI)

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `backend/python/migrations/046_mcp_db_table_rules.sql` | Create | ALTER TABLE adds JSONB column |
| `backend/python/web/models/api_key.py` | Modify | Add column + helper method |
| `mcp_esa/server/auth.py` | Modify | Include `mcp_db_table_rules` in auth response |
| `mcp_esa/server/transport.py` | Modify | New contextvar, set from request.state |
| `mcp_esa/services/database_service.py` | Modify | Add `extract_table_references()` method |
| `mcp_esa/tools/database_tools.py` | Modify | Enforce table restrictions in 3 tools |
| `backend/python/web/routes/admin.py` | Modify | New AJAX endpoint + save logic for table rules |
| `backend/python/web/templates/admin/api_keys/edit.html` | Modify | Dynamic table checkboxes per preset |

---

### Task 1: Database Migration

**Files:**
- Create: `backend/python/migrations/046_mcp_db_table_rules.sql`

- [ ] **Step 1: Create migration file**

```sql
-- 046: Add table-level access control for MCP database presets
-- mcp_db_table_rules: {"preset_name": ["table1", "table2"], ...}
-- Empty object ({}) = no table restrictions (all tables allowed)
-- Preset key with list = only those tables are accessible

ALTER TABLE api_keys
ADD COLUMN IF NOT EXISTS mcp_db_table_rules JSONB NOT NULL DEFAULT '{}'::jsonb;

COMMENT ON COLUMN api_keys.mcp_db_table_rules IS 'Per-preset table allow-lists for MCP DB tools. Empty = no restrictions.';
```

- [ ] **Step 2: Run migration on esa_backend DB**

```bash
DB_PW=$(python3 -c "from dotenv import load_dotenv; load_dotenv('.env'); import os; print(os.environ['DB_PASSWORD'])")
PGPASSWORD="$DB_PW" psql "host=esapbi.postgres.database.azure.com port=5432 dbname=backend user=esa_pbi_admin sslmode=require" -f backend/python/migrations/046_mcp_db_table_rules.sql
```

Expected: `ALTER TABLE` success.

- [ ] **Step 3: Commit**

```bash
git add backend/python/migrations/046_mcp_db_table_rules.sql
git commit -m "feat: add mcp_db_table_rules column for table-level MCP access control"
```

---

### Task 2: Model & Auth Pipeline

**Files:**
- Modify: `backend/python/web/models/api_key.py` (add column at line ~95, helper method, update `to_dict`)
- Modify: `mcp_esa/server/auth.py` (two SQL queries + two auth response dicts)
- Modify: `mcp_esa/server/transport.py` (new contextvar + set it in `handle_mcp_request`)

- [ ] **Step 1: Update ApiKey model**

In `backend/python/web/models/api_key.py`, after the `mcp_db_presets` column (line 95), add:

```python
mcp_db_table_rules = Column(JSONB, nullable=False, default=dict,
                             comment="Per-preset table allow-lists (empty = all tables)")
```

Add a helper method after `has_mcp_tool_access` (after line 133):

```python
def get_allowed_tables(self, preset_name):
    """Get allowed tables for a preset. Returns None if no restrictions."""
    if not self.mcp_db_table_rules:
        return None
    tables = self.mcp_db_table_rules.get(preset_name)
    if not tables:
        return None  # Empty list or missing key = no restrictions
    return tables
```

Update `to_dict()` to include the new field (add after `mcp_db_presets` line):

```python
'mcp_db_table_rules': self.mcp_db_table_rules or {},
```

- [ ] **Step 2: Update auth.py — API key auth query**

In `mcp_esa/server/auth.py`, `_authenticate_api_key()` function:

Update the SELECT query (line ~225) to include `ak.mcp_db_table_rules`:

```sql
SELECT ak.id, ak.key_hash, ak.scopes, ak.is_active,
       ak.expires_at, ak.daily_quota, ak.daily_usage, ak.quota_reset_date,
       ak.mcp_enabled, ak.mcp_tools, ak.mcp_db_presets, ak.mcp_db_table_rules,
       u.username, u.id as user_id
FROM api_keys ak
JOIN users u ON u.id = ak.user_id
WHERE ak.key_id = :key_id
```

Update the return dict (line ~282) to include:

```python
"mcp_db_table_rules": row.mcp_db_table_rules or {},
```

- [ ] **Step 3: Update auth.py — OAuth bearer token lookup**

In `_authenticate_bearer_token()`, update the SELECT query (line ~358) to include `ak.mcp_db_table_rules`:

```sql
SELECT ak.mcp_tools, ak.mcp_db_presets, ak.mcp_db_table_rules, u.username
FROM api_keys ak JOIN users u ON u.id = ak.user_id
WHERE ak.key_id = :key_id AND ak.is_active = true AND ak.mcp_enabled = true
```

Add `mcp_db_table_rules = {}` initialization alongside `mcp_db_presets = []` (line ~353).

After the existing `mcp_db_presets` assignment (line ~369), add:

```python
mcp_db_table_rules = row.mcp_db_table_rules or {}
```

Update the single return dict (line ~377) to include `"mcp_db_table_rules": mcp_db_table_rules`.

- [ ] **Step 4: Update auth middleware to pass table rules to request.state**

In `mcp_esa/server/auth.py`, in the middleware function, after each line that sets `request.state.mcp_db_presets` (lines 144 and 165), add:

```python
request.state.mcp_db_table_rules = user_info.get("mcp_db_table_rules", {})
```

- [ ] **Step 5: Add contextvar in transport.py and set it**

In `mcp_esa/server/transport.py`, after line 28, add:

```python
allowed_db_tables_var: contextvars.ContextVar[dict] = contextvars.ContextVar('allowed_db_tables', default={})
```

In `handle_mcp_request()`, after line 51 (`allowed_db_presets_var.set(db_presets)`), add:

```python
# Set per-preset table restrictions in contextvar for DB tools
db_table_rules = getattr(request.state, 'mcp_db_table_rules', {}) if hasattr(request, 'state') else {}
allowed_db_tables_var.set(db_table_rules)
```

- [ ] **Step 6: Commit**

```bash
git add backend/python/web/models/api_key.py mcp_esa/server/auth.py mcp_esa/server/transport.py
git commit -m "feat: pipe mcp_db_table_rules through auth → transport → contextvar"
```

---

### Task 3: Enforcement in Database Tools

**Files:**
- Modify: `mcp_esa/services/database_service.py` (add table extraction method)
- Modify: `mcp_esa/tools/database_tools.py` (enforce in 3 tools)

- [ ] **Step 1: Add table reference extractor in database_service.py**

Add this method to the `DatabaseService` class (after `_categorize_execution_time`, line ~230):

```python
@staticmethod
def extract_table_references(query: str) -> set:
    """
    Extract table names referenced in a SQL query using regex.
    Catches FROM, JOIN, and INTO clauses. Not a full SQL parser,
    but sufficient for read-only SELECT queries.
    """
    # Normalize whitespace
    normalized = re.sub(r'\s+', ' ', query.strip())

    tables = set()

    # Strip double-quoted identifiers to plain names (prevents bypass via "Table")
    normalized = re.sub(r'"([^"]+)"', r'\1', normalized)

    # Match: FROM table, JOIN table, INTO table
    # Handles optional schema prefix (schema.table)
    # Stops at whitespace, comma, parenthesis, or semicolon
    pattern = r'(?:FROM|JOIN|INTO)\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)(?:\s|,|\(|;|$)'
    matches = re.findall(pattern, normalized, re.IGNORECASE)

    # System schemas that should be blocked when table restrictions are active
    blocked_schemas = {'information_schema', 'pg_catalog'}

    for match in matches:
        # If schema.table, check for system schema access
        if '.' in match:
            schema_part, table_part = match.rsplit('.', 1)
            if schema_part.lower() in blocked_schemas:
                # Mark as a system schema access — caller should block
                tables.add(f'_system_.{table_part.lower()}')
            else:
                tables.add(table_part.lower())
        else:
            # Skip SQL keywords that can follow FROM/JOIN
            if match.upper() not in ('SELECT', 'LATERAL', 'UNNEST', 'GENERATE_SERIES', 'VALUES'):
                tables.add(match.lower())

    return tables
```

- [ ] **Step 2: Update database_tools.py — import new contextvar**

In `mcp_esa/tools/database_tools.py`, update line 15 import:

```python
from mcp_esa.server.transport import allowed_db_presets_var, allowed_db_tables_var
```

- [ ] **Step 3: Add helper function in database_tools.py**

Add this helper after the `_active_connections` dict (after line 23):

```python
def _get_allowed_tables(connection_name: str) -> list:
    """Get the allowed tables for a connection. Returns None if no restrictions."""
    table_rules = allowed_db_tables_var.get({})
    if not table_rules:
        return None
    tables = table_rules.get(connection_name)
    if not tables:
        return None  # Missing key or empty list = no restrictions
    return [t.lower() for t in tables]
```

- [ ] **Step 4: Enforce in DB_list_tables**

In the `DB_list_tables` function, after `tables = await conn.get_tables(schema)` (line 202), add filtering:

```python
            # Filter by per-key table restrictions
            allowed_tables = _get_allowed_tables(connection_name)
            if allowed_tables is not None:
                filtered = []
                for table in tables:
                    if isinstance(table, dict):
                        name = table.get('table_name') or table.get('TABLE_NAME') or list(table.values())[0]
                    else:
                        name = str(table)
                    if name.lower() in allowed_tables:
                        filtered.append(table)
                tables = filtered
```

- [ ] **Step 5: Enforce in DB_describe_table**

In the `DB_describe_table` function, after the connection check (after line 240), add:

```python
        # Check per-key table restrictions
        allowed_tables = _get_allowed_tables(connection_name)
        if allowed_tables is not None and table_name.lower() not in allowed_tables:
            return f"Access denied: table '{table_name}' is not accessible for this API key"
```

- [ ] **Step 6: Enforce in DB_execute_query**

In the `DB_execute_query` function, after the connection check and before `result = await db_service.execute_safe_query(conn, query)` (line 149), add:

```python
        # Check per-key table restrictions
        allowed_tables = _get_allowed_tables(connection_name)
        if allowed_tables is not None:
            referenced = db_service.extract_table_references(query)
            # Block system schema access (information_schema, pg_catalog)
            system_refs = {t for t in referenced if t.startswith('_system_.')}
            if system_refs:
                return "Access denied: system catalog queries are not allowed when table restrictions are active"
            blocked = referenced - set(allowed_tables)
            if blocked:
                blocked_list = ', '.join(sorted(blocked))
                return f"Access denied: query references restricted table(s): {blocked_list}"
```

- [ ] **Step 7: Commit**

```bash
git add mcp_esa/services/database_service.py mcp_esa/tools/database_tools.py
git commit -m "feat: enforce table-level access in DB_list_tables, DB_describe_table, DB_execute_query"
```

---

### Task 4: Admin AJAX Endpoint for Live Table Lists

**Files:**
- Modify: `backend/python/web/routes/admin.py` (new endpoint + update save logic)

- [ ] **Step 1: Add AJAX endpoint to list tables for a preset**

Add this route in `admin.py` near the existing `_get_mcp_db_presets()` helper (after line ~1050):

```python
@admin_bp.route('/api-keys/preset-tables/<preset_name>', methods=['GET'])
@login_required
@admin_required
def get_preset_tables(preset_name):
    """AJAX: Return list of tables in a database preset (for table access control UI)."""
    import re
    try:
        from common.config_loader import get_config
        config = get_config()
        raw = config.get_raw_config('mcp')
        databases = raw.get('databases', {})

        if preset_name not in databases:
            return jsonify({"error": "Preset not found"}), 404

        db_config = databases[preset_name]
        db_type = db_config.get('type', 'postgresql')

        # Resolve password from vault
        password = None
        pw_key = db_config.get('password_vault')
        if pw_key:
            from common.secrets_vault import vault_config
            password = vault_config(pw_key, default=None)
        else:
            password = db_config.get('password')

        tables = []

        if db_type == 'bigquery':
            # BigQuery: use google-cloud-bigquery
            creds_key = db_config.get('credentials_json_vault')
            if creds_key:
                import json as json_mod
                from common.secrets_vault import vault_config
                from google.cloud import bigquery
                from google.oauth2 import service_account
                creds_json = vault_config(creds_key, default=None)
                if creds_json:
                    info = json_mod.loads(creds_json)
                    credentials = service_account.Credentials.from_service_account_info(info)
                    client = bigquery.Client(credentials=credentials, project=db_config.get('project_id'))
                    dataset = db_config.get('dataset')
                    if dataset:
                        for t in client.list_tables(dataset):
                            tables.append(t.table_id)
        else:
            # PostgreSQL / MySQL / MariaDB / MSSQL — use SQLAlchemy
            from sqlalchemy import create_engine, text as sa_text

            if db_type == 'postgresql':
                ssl_param = '?sslmode=require' if db_config.get('ssl') else ''
                url = f"postgresql://{db_config['user']}:{password}@{db_config['host']}:{db_config.get('port', 5432)}/{db_config['database']}{ssl_param}"
            elif db_type in ('mysql', 'mariadb'):
                url = f"mysql+pymysql://{db_config['user']}:{password}@{db_config['host']}:{db_config.get('port', 3306)}/{db_config['database']}"
            elif db_type == 'mssql':
                url = f"mssql+pyodbc://{db_config['user']}:{password}@{db_config['host']}:{db_config.get('port', 1433)}/{db_config['database']}?driver=ODBC+Driver+17+for+SQL+Server"
            else:
                return jsonify({"error": f"Unsupported DB type: {db_type}"}), 400

            engine = create_engine(url)
            with engine.connect() as conn:
                if db_type == 'postgresql':
                    rows = conn.execute(sa_text(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema = 'public' AND table_type IN ('BASE TABLE', 'VIEW') "
                        "ORDER BY table_name"
                    ))
                elif db_type in ('mysql', 'mariadb'):
                    rows = conn.execute(sa_text(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema = :db ORDER BY table_name"
                    ), {"db": db_config['database']})
                elif db_type == 'mssql':
                    rows = conn.execute(sa_text(
                        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                        "WHERE TABLE_SCHEMA = 'dbo' AND TABLE_TYPE = 'BASE TABLE' "
                        "ORDER BY TABLE_NAME"
                    ))
                tables = [row[0] for row in rows]
            engine.dispose()

        # Sanitize: only return valid identifier names
        safe_tables = [t for t in tables if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]{0,127}$', t)]
        return jsonify({"tables": sorted(safe_tables)})

    except Exception as e:
        logger.error(f"Failed to list tables for preset {preset_name}: {e}")
        return jsonify({"error": "Failed to connect to database"}), 500
```

- [ ] **Step 2: Update save logic for mcp_db_table_rules**

In the `edit_api_key` POST handler, after the `mcp_db_presets` save (line ~1148), add:

```python
            # Update MCP DB table restrictions
            # Form sends: mcp_db_table_rules__esa_pbi=rent_rolls&mcp_db_table_rules__esa_pbi=site_info&...
            mcp_db_table_rules = {}
            for key in request.form:
                if key.startswith('mcp_db_table_rules__'):
                    preset = key[len('mcp_db_table_rules__'):]
                    tables = request.form.getlist(key)
                    if tables:
                        mcp_db_table_rules[preset] = tables
            api_key.mcp_db_table_rules = mcp_db_table_rules
```

Update the audit_log line to also include table rules count:

```python
            audit_log(AuditEvent.CONFIG_UPDATED,
                      f"Updated API key config for user '{user.username}': scopes={scopes}, "
                      f"rate_limit={api_key.rate_limit}, daily_quota={api_key.daily_quota}, "
                      f"mcp_enabled={api_key.mcp_enabled}, mcp_tools={len(mcp_tools)} selected, "
                      f"mcp_db_presets={len(mcp_db_presets)} selected, "
                      f"mcp_db_table_rules={len(mcp_db_table_rules)} preset(s) restricted")
```

- [ ] **Step 3: Commit**

```bash
git add backend/python/web/routes/admin.py
git commit -m "feat: admin AJAX endpoint for live table listing + save table rules"
```

---

### Task 5: Admin UI — Dynamic Table Checkboxes

**Files:**
- Modify: `backend/python/web/templates/admin/api_keys/edit.html`

- [ ] **Step 1: Add table restriction UI section**

In `edit.html`, replace the existing DB presets section (lines 137–156) with an enhanced version that includes expandable table access per preset:

```html
        {% if mcp_db_presets_available %}
        <div id="mcp-presets-section" style="margin-top: 1rem; padding-top: 0.75rem; border-top: 1px solid var(--esa-gray-dark); {{ 'display: none;' if not api_key.mcp_enabled }}">
            <label style="font-weight: 600; font-size: 0.85rem; color: var(--esa-navy); margin-bottom: 0.25rem; display: block;">Database Preset Access</label>
            <p style="font-size: 0.8rem; color: #888; margin-bottom: 0.5rem;">
                Leave all unchecked to grant access to <strong>all presets</strong>. Check specific presets to restrict access.
            </p>
            <div class="scope-grid">
                {% for preset_name, preset_info in mcp_db_presets_available.items() %}
                <div>
                    <label style="font-size: 0.8rem;">
                        <input type="checkbox" name="mcp_db_presets" value="{{ preset_name }}"
                               class="mcp-preset-cb" data-preset="{{ preset_name }}"
                               {{ 'checked' if api_key.mcp_db_presets and preset_name in api_key.mcp_db_presets }}>
                        <code style="font-size: 0.75rem;">{{ preset_name }}</code>
                        <span style="color: #888; font-size: 0.7rem;">({{ preset_info }})</span>
                    </label>
                    <!-- Expandable table access section -->
                    <div class="table-access-section" data-preset="{{ preset_name }}"
                         style="margin-left: 1.4rem; margin-top: 0.4rem; display: none;">
                        <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.3rem;">
                            <button type="button" class="btn-load-tables" data-preset="{{ preset_name }}"
                                    style="font-size: 0.7rem; padding: 0.15rem 0.4rem; background: var(--esa-navy); color: white; border: none; border-radius: 3px; cursor: pointer;">
                                Load Tables
                            </button>
                            <span class="table-status" data-preset="{{ preset_name }}" style="font-size: 0.7rem; color: #888;"></span>
                        </div>
                        <p style="font-size: 0.7rem; color: #aaa; margin-bottom: 0.3rem;">
                            Leave all unchecked = <strong>all tables allowed</strong> (no restriction). Check specific tables to restrict access to only those tables.
                        </p>
                        <div class="table-checkboxes" data-preset="{{ preset_name }}"
                             style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 0.2rem 1rem; max-height: 300px; overflow-y: auto;">
                            <!-- Populated by AJAX -->
                        </div>
                    </div>
                </div>
                {% endfor %}
            </div>
        </div>
        {% endif %}
```

- [ ] **Step 2: Add JavaScript for dynamic table loading**

In the `{% block scripts %}` section, add the table loading logic. The existing table rules from the API key need to be embedded as JSON for pre-checking:

```html
<!-- Embed existing table rules for pre-checking -->
<script>
const existingTableRules = {{ (api_key.mcp_db_table_rules or {})|tojson }};
</script>
```

Then add the table loading functions:

```javascript
// Show/hide table access section when preset checkbox changes
document.querySelectorAll('.mcp-preset-cb').forEach(function(cb) {
    var preset = cb.dataset.preset;
    var section = document.querySelector('.table-access-section[data-preset="' + preset + '"]');
    if (section) {
        section.style.display = cb.checked ? '' : 'none';
        // Auto-load tables if preset is checked and has existing rules
        if (cb.checked && existingTableRules[preset]) {
            loadTablesForPreset(preset);
        }
    }
    cb.addEventListener('change', function() {
        if (section) section.style.display = this.checked ? '' : 'none';
    });
});

function loadTablesForPreset(preset) {
    var container = document.querySelector('.table-checkboxes[data-preset="' + preset + '"]');
    var status = document.querySelector('.table-status[data-preset="' + preset + '"]');
    var btn = document.querySelector('.btn-load-tables[data-preset="' + preset + '"]');
    if (!container) return;

    status.textContent = 'Loading...';
    btn.disabled = true;

    fetch('/admin/api-keys/preset-tables/' + encodeURIComponent(preset))
        .then(function(r) { return r.json(); })
        .then(function(data) {
            if (data.error) {
                status.textContent = 'Error: ' + data.error;
                btn.disabled = false;
                return;
            }
            var tables = data.tables || [];
            var existing = existingTableRules[preset] || [];
            var existingLower = existing.map(function(t) { return t.toLowerCase(); });

            container.innerHTML = '';
            if (tables.length === 0) {
                status.textContent = 'No tables found';
                btn.disabled = false;
                return;
            }

            // Select/Deselect All buttons
            var controls = document.createElement('div');
            controls.style.cssText = 'grid-column: 1 / -1; margin-bottom: 0.3rem; display: flex; gap: 0.5rem;';
            controls.innerHTML =
                '<button type="button" onclick="togglePresetTables(\'' + preset + '\', true)" style="font-size: 0.65rem; padding: 0.1rem 0.3rem; border: 1px solid #ccc; background: #f5f5f5; border-radius: 2px; cursor: pointer;">Select All</button>' +
                '<button type="button" onclick="togglePresetTables(\'' + preset + '\', false)" style="font-size: 0.65rem; padding: 0.1rem 0.3rem; border: 1px solid #ccc; background: #f5f5f5; border-radius: 2px; cursor: pointer;">Deselect All</button>';
            container.appendChild(controls);

            tables.forEach(function(table) {
                var checked = existingLower.indexOf(table.toLowerCase()) !== -1;
                var div = document.createElement('div');
                div.innerHTML =
                    '<label style="font-size: 0.75rem; display: flex; align-items: center; gap: 0.3rem; cursor: pointer;">' +
                    '<input type="checkbox" name="mcp_db_table_rules__' + preset + '" value="' + table + '"' +
                    (checked ? ' checked' : '') + ' class="mcp-table-cb" data-preset="' + preset + '">' +
                    '<code style="font-size: 0.7rem;">' + table + '</code></label>';
                container.appendChild(div);
            });

            status.textContent = tables.length + ' tables';
            btn.disabled = false;
        })
        .catch(function(err) {
            status.textContent = 'Failed to load';
            btn.disabled = false;
        });
}

function togglePresetTables(preset, checked) {
    document.querySelectorAll('.mcp-table-cb[data-preset="' + preset + '"]')
        .forEach(function(cb) { cb.checked = checked; });
}

// Wire up Load Tables buttons
document.querySelectorAll('.btn-load-tables').forEach(function(btn) {
    btn.addEventListener('click', function() {
        loadTablesForPreset(this.dataset.preset);
    });
});
```

- [ ] **Step 3: Commit**

```bash
git add backend/python/web/templates/admin/api_keys/edit.html
git commit -m "feat: admin UI dynamic table checkboxes per DB preset"
```

---

### Task 6: Smoke Test & Verify

- [ ] **Step 1: Verify migration applied**

```bash
DB_PW=$(python3 -c "from dotenv import load_dotenv; load_dotenv('.env'); import os; print(os.environ['DB_PASSWORD'])")
PGPASSWORD="$DB_PW" psql "host=esapbi.postgres.database.azure.com port=5432 dbname=backend user=esa_pbi_admin sslmode=require" -c "\d api_keys" | grep mcp_db_table_rules
```

Expected: column exists with type `jsonb`.

- [ ] **Step 2: Test AJAX endpoint**

Start the Flask dev server, log in as admin, navigate to an API key edit page. Click a preset checkbox → click "Load Tables" → verify table list loads dynamically.

- [ ] **Step 3: Test save round-trip**

Check some table checkboxes, save. Re-open the edit page → verify the checked presets auto-load their tables and pre-check the saved ones.

- [ ] **Step 4: Test MCP enforcement**

Using an API key with table restrictions set, test via MCP:
1. `DB_connect_preset("esa_pbi")` → success
2. `DB_list_tables("esa_pbi")` → only allowed tables shown
3. `DB_describe_table("esa_pbi", "restricted_table")` → "Access denied"
4. `DB_execute_query("esa_pbi", "SELECT * FROM restricted_table")` → "Access denied: query references restricted table(s)"
5. `DB_execute_query("esa_pbi", "SELECT * FROM allowed_table LIMIT 1")` → success

- [ ] **Step 5: Final commit**

```bash
git add -A
git commit -m "feat: table-level access control for MCP database presets — complete"
```
