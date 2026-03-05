# New Tool Page

Scaffold a new tool page following the established pattern: permission decorator + route + template.

Usage: `/new-tool <tool-name> <description>`
Example: `/new-tool unit-search Search units across all sites by name or ID`

Tool name: $ARGUMENTS

---

## Step 1: Parse Input

Extract from the arguments:
- **tool_name**: kebab-case name (e.g., `unit-search`)
- **snake_name**: snake_case version (e.g., `unit_search`)
- **display_name**: Title Case for UI (e.g., `Unit Search`)
- **description**: what the tool does

---

## Step 2: Add Permission to Roles Table

Read `backend/python/web/models/role.py` to understand the existing permission columns.

Create a SQL migration file in `backend/python/migrations/` that adds the new permission column:
```sql
ALTER TABLE roles ADD COLUMN can_access_<snake_name>_tools BOOLEAN NOT NULL DEFAULT false;
UPDATE roles SET can_access_<snake_name>_tools = true WHERE name = 'Admin';
```

---

## Step 3: Add Permission Decorator

Read `backend/python/web/auth/decorators.py` to see existing tool decorators (e.g., `billing_tools_access_required`, `inventory_tools_access_required`).

Add a new decorator following the exact same pattern:
```python
def <snake_name>_tools_access_required(f):
    ...
```

Also update `backend/python/web/models/role.py` to add the `can_access_<snake_name>_tools` column to the Role model.

And update `backend/python/web/models/user.py` to add the `can_access_<snake_name>_tools()` permission check method (follow existing pattern).

---

## Step 4: Add Route

Read `backend/python/web/routes/tools.py` and add the new route following the existing pattern:

```python
@tools_bp.route('/<tool-name>')
@login_required
@<snake_name>_tools_access_required
def <snake_name>():
    """<Display Name> tool page."""
    return render_template('tools/<snake_name>.html')
```

---

## Step 5: Create Template

Read one existing tool template (e.g., `backend/python/web/templates/tools/billing_date_changer.html`) to understand the structure.

Create `backend/python/web/templates/tools/<snake_name>.html` with:
- Extends `base.html`
- Title block with display name
- Content block with basic layout (site selector, data area, action buttons as appropriate)
- Scripts block with fetch-based API integration skeleton
- Loading states and error handling

Keep it minimal — just the scaffold with placeholders for the actual logic.

---

## Step 6: Summary

Report what was created:
1. Migration file path
2. Decorator added to decorators.py
3. Role model updated
4. User model updated
5. Route added to tools.py
6. Template created

Remind the user to:
- Run the migration on the database
- Grant the permission to appropriate roles via Admin UI
- Implement the actual API endpoints the tool will call
- Fill in the template JS logic

---

## Rules
- Follow EXACTLY the patterns in existing tool pages — no new conventions
- Keep the template minimal — scaffold only, not full implementation
- Don't create API endpoints — that's a separate task
- Don't add npm/build dependencies
- Use vanilla JS with fetch API in the template
