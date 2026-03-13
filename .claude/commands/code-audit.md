# Code Audit

Review code for readability, consistency, and adherence to project conventions. Finds and fixes style issues, not security bugs (use `/security-review` for that).

Usage: `/code-audit <scope>` or `/code-audit` (defaults to recently modified files)
Example: `/code-audit backend/python/web/routes/api.py`

Scope: $ARGUMENTS
If no scope is provided, audit all recently modified files (use `git diff --name-only` and `git ls-files --others --exclude-standard`).

---

## Standards Checklist

Audit against these project-specific standards:

### Python (Backend)

1. **Logging** — `logging.getLogger(__name__)`, never `print()`. Log levels used correctly (debug/info/warning/error).
2. **Error handling** — Generic messages to client (`"An error occurred"`), real error to logger. Never leak `str(e)` in responses.
3. **DB sessions** — Always closed in `finally` block. Use `current_app.get_db_session()` for esa_backend, lazy engine pattern for esa_pbi.
4. **SQL** — ORM or parameterized queries only. No f-strings or `.format()` in SQL.
5. **Naming** — `snake_case` for functions/variables, `PascalCase` for classes, `UPPER_CASE` for constants. Descriptive names (no single-letter vars except loop counters).
6. **Imports** — Standard library first, then third-party, then project imports. No unused imports.
7. **Route patterns** — Blueprints with proper decorators (`@login_required`, `@require_auth`, `@rate_limit_api`). Consistent response format (`jsonify({"status": "success", "data": ...})`).
8. **Functions** — Reasonable length (flag functions >50 lines as candidates for extraction). Single responsibility.
9. **Dead code** — No commented-out code blocks, unused variables, unreachable code.
10. **Magic values** — No unexplained hardcoded numbers or strings. Use constants or config.

### JavaScript (Frontend Templates)

1. **Consistency** — `const`/`let` (no `var`). Consistent quote style. Descriptive function/variable names.
2. **Error handling** — `fetch` calls have `.catch()` or try/catch. User-facing error messages shown in UI.
3. **DOM** — Use `getElementById`/`querySelector` consistently. No inline `onclick` attributes (use `addEventListener`).
4. **No frameworks** — Vanilla JS only. No jQuery, React, Vue references.

### Performance & Efficiency

1. **N+1 queries** — Flag loops that issue a DB query per iteration. Use eager loading (`joinedload`, `subqueryload`) or batch queries instead.
2. **DB session scope** — Sessions opened too early or held open during SOAP/HTTP calls. Open late, close early.
3. **Unbounded queries** — `query.all()` without `.limit()` on user-facing endpoints. Flag if the table can grow large.
4. **Missing indexes** — Queries filtering on columns not covered by an index (check `__table_args__` and migration files).
5. **Redundant queries** — Same data fetched multiple times in one request when it could be passed through or cached in a local variable.
6. **SOAP call efficiency** — Multiple sequential SOAP calls that could be batched or parallelized with `concurrent.futures`.
7. **Large response payloads** — Endpoints returning full objects when only a subset of fields is needed. Flag if response could be trimmed.
8. **Memory** — Loading entire result sets into memory (e.g., `list(generator)`) when streaming/chunking would work. Flag large `fetchall()` without pagination.
9. **Connection leaks** — SOAP clients, DB engines, or HTTP sessions not closed in `finally` blocks.
10. **Frontend fetch** — Redundant API calls on page load, missing debounce on search/filter inputs, large DOM rebuilds when incremental updates would suffice.

### General

1. **No TODO/FIXME/HACK comments** left unaddressed — flag them.
2. **Consistent indentation** — 4 spaces for Python, 2 or 4 spaces for JS/HTML (but consistent within a file).
3. **File length** — Flag files >500 lines as candidates for splitting (informational, not auto-fixed).

---

## Step 1: Audit

Read all files in scope. For each file, check against the standards checklist above. Classify each finding as:

- **STYLE** — Naming, formatting, consistency issues
- **READABILITY** — Hard-to-follow logic, overly complex expressions, missing context
- **CONVENTION** — Deviates from project patterns documented in CLAUDE.md
- **CLEANUP** — Dead code, unused imports, TODO comments, magic values
- **PERFORMANCE** — N+1 queries, unbounded fetches, connection leaks, redundant calls, missing indexes

---

## Step 2: Findings Report

Present a summary table:

```
| # | Type       | File:Line | Issue                              | Auto-fix? |
|---|------------|-----------|------------------------------------|-----------|
| 1 | CONVENTION | ...       | Uses print() instead of logger     | Yes       |
| 2 | READABILITY| ...       | Function is 80 lines, hard to follow| No       |
```

Then group findings by file with specifics and proposed fixes.

If there are **no findings** → report the code is clean and STOP.

---

## Step 3: Ask Before Fixing

Present the findings and ask: **"Do you want me to fix the auto-fixable issues? (Y/N)"**

- If **No** → STOP. The report is the deliverable.
- If **Yes** → Proceed to Step 4.

---

## Step 4: Apply Fixes

Fix only the items marked "Auto-fix: Yes". This includes:
- Replacing `print()` with proper logging
- Removing unused imports
- Fixing naming inconsistencies
- Removing dead/commented-out code
- Replacing magic values with named constants
- Standardizing response formats
- Adding missing `finally: session.close()` / `soap_client.close()` for connection leaks
- Adding `.limit()` to unbounded queries on user-facing endpoints

**Do NOT auto-fix:**
- Large function refactoring (just flag it)
- File splitting (just flag it)
- N+1 query restructuring (just flag it — requires testing)
- Adding DB indexes (just flag it — requires migration)
- Parallelizing SOAP calls (just flag it — requires testing)
- Logic changes (just flag it)
- Anything that changes behavior

---

## Step 5: Summary

Show what was fixed:
```
Fixed 5/8 issues:
✓ #1 — Replaced print() with logger in api.py:245
✓ #3 — Removed unused import in tools.py:2
...
Remaining (manual review needed):
○ #2 — api.py: consider splitting get_unit_availability (80 lines)
```

---

## Rules
- This is about readability and consistency, NOT security (use `/security-review` for that)
- NEVER change logic or behavior — only style and convention fixes
- NEVER add docstrings, type annotations, or comments to code the user didn't ask to change
- Preserve the existing code patterns — enforce consistency, don't impose new patterns
- Ask before making any changes
