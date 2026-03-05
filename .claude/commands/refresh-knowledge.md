# Refresh Knowledge

Scan the codebase and update Claude Code configuration files to reflect the current state of the project. Run this periodically or after major changes.

Usage: `/refresh-knowledge` or `/refresh-knowledge <focus area>`
Example: `/refresh-knowledge routes` — focus on route/API changes
Example: `/refresh-knowledge all` — full scan

Focus: $ARGUMENTS (default: all)

---

## Step 1: Scan Current State

Read the following to understand what currently exists:

**Project structure:**
- `ls` the top-level directory, `backend/python/`, `backend/python/web/routes/`, `backend/python/web/models/`, `backend/python/web/templates/tools/`, `backend/python/common/`, `backend/python/config/`, `scripts/`
- Count lines in major files (api.py, etc.) to track growth

**Configuration files:**
- `backend/python/config/*.yaml` — check for new/removed config files
- `backend/python/common/config_loader.py` — check for config pattern changes

**Routes & Blueprints:**
- Read the first 30 lines of every file in `backend/python/web/routes/` to capture blueprint names, prefixes, and imports
- Check for new route files or removed ones

**Models:**
- Read `backend/python/common/models.py` first 100 lines for shared models
- `ls backend/python/web/models/` for app models
- Check for new model files

**Auth & Security:**
- Read `backend/python/web/auth/decorators.py` for current permission decorators
- Read `backend/python/web/auth/jwt_auth.py` first 60 lines for auth patterns
- Check `backend/python/web/utils/` for new utilities

**Templates & Tools:**
- `ls backend/python/web/templates/tools/` for tool pages
- `ls backend/python/web/templates/` for template structure

**External integrations:**
- Check `backend/python/common/` for new client modules (soap_client, sugarcrm_client, http_client, etc.)

**Deploy & Scripts:**
- `ls scripts/` for new scripts
- Read `scripts/deploy_to_vm.py` first 30 lines for deploy pattern changes

**Vault secrets:**
- Read `backend/python/common/secrets_vault.py` SENSITIVE_KEYS list to catch new secrets

---

## Step 2: Read Current Config Files

Read all of these to know what's currently documented:
- `/home/louis/PycharmProjects/WebApp_BackendESA/CLAUDE.md`
- `/home/louis/.claude/projects/-home-louis-PycharmProjects-WebApp-BackendESA/memory/MEMORY.md`
- `/home/louis/.claude/projects/-home-louis-PycharmProjects-WebApp-BackendESA/memory/*.md` (any topic files)
- `.claude/knowledge/credentials_paths.md`

---

## Step 3: Diff & Report

Compare what you found in Step 1 against what's documented in Step 2. Present a clear report:

```
## Knowledge Drift Report

### New (not documented)
- [list items found in code but missing from docs]

### Changed (documented but outdated)
- [list items where docs don't match current code]

### Removed (documented but no longer exists)
- [list items in docs that no longer exist in code]

### Unchanged
- [count of items that are still accurate]
```

If the focus area was specified, only report on that area.

---

## Step 4: Ask Before Updating

Present the report and ask: **"Do you want me to apply these updates to CLAUDE.md and MEMORY.md?"**

- If **No** → Stop. Done.
- If **Yes** → Proceed to Step 5.
- If the user wants selective updates → Only update what they approve.

---

## Step 5: Apply Updates

Update the following files with the changes identified:

1. **`CLAUDE.md` (project root)** — Update project structure, tech stack, conventions, file lists. Keep it concise and accurate.
2. **`memory/MEMORY.md`** — Update architecture refs, key file locations, vault secrets list. Stay under 200 lines.
3. **`.claude/knowledge/credentials_paths.md`** — Update if paths, site IDs, or DB patterns changed.
4. **Agent files** — If major architectural changes were found (new framework, new auth pattern, new DB), flag which agents need updating but don't auto-update them. Report: "These agents may need updating: [list]"

---

## Rules
- NEVER delete information you're unsure about — flag it for review instead
- NEVER update .env, vault files, or credentials
- Keep CLAUDE.md under 120 lines — it's a quick reference, not documentation
- Keep MEMORY.md under 200 lines (hard limit — gets truncated beyond that)
- Prefer updating existing sections over adding new ones
- If something looks intentionally different from the code (e.g., a known workaround), leave it and note it in the report
