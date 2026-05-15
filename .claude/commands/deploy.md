# Deploy

Secure deployment pipeline: Security Check → Code Audit → Commit → Push → Deploy. Stop immediately if any gate fails.

Usage: `/deploy <commit context or description of changes>`
Example: `/deploy added inventory filtering and batch actions`

Context for this commit: $ARGUMENTS

---

## Step 1: Security Gate — Pentest Review

Use the **pentest-code-reviewer** agent (via the Task tool with `subagent_type: "pentest-code-reviewer"`) to review ALL staged and modified files (use `git diff` and `git diff --cached` to identify them).

The agent must check for CRITICAL, HIGH, and MEDIUM severity vulnerabilities.

### Decision:
- **If any CRITICAL, HIGH, or MEDIUM findings exist**: Report them clearly, do NOT proceed to commit. Stop here.
- **If only LOW/INFO or no findings**: Proceed to Step 2.

---

## Step 2: Code Quality Gate — Code Audit

Audit all staged and modified files against project conventions (logging, naming, error handling, DB patterns, dead code, magic values).

Check the standards from `/code-audit`:
- Python: proper logging (no `print()`), error handling (no `str(e)` leaks), DB session cleanup, naming conventions, no unused imports, no dead code, no magic values
- JavaScript: `const`/`let` (no `var`), fetch error handling, consistent style
- General: no unaddressed TODO/FIXME, consistent indentation

### Decision:
- **If CONVENTION or CLEANUP issues found**: List them, auto-fix the safe ones (unused imports, print→logger, dead code removal), and report what was fixed. Then proceed to Step 3.
- **If READABILITY issues found** (e.g., functions >50 lines): Flag them for awareness but do NOT block — proceed to Step 3.
- **If code is clean**: Proceed to Step 3.

---

## Step 3: Commit & Push

1. Stage all relevant changed files (be selective — never stage `.env`, credentials, or secrets).
2. Create a commit with a descriptive message based on the actual changes and the context provided above.
3. Push to `origin` on the current branch.
4. Confirm push succeeded before continuing.

---

## Step 4: Ask About VM Deployment

Ask the user: **"Push successful. Do you want to deploy to the VM now?"**

- If **No** → Stop here. Done.
- If **Yes** → Proceed to Step 5.

---

## Step 5: Deploy to VM

**ALWAYS use `scripts/deploy_to_vm.py`.** The VM has no git — code is shipped via rsync from the dev machine. The VM's `/var/www/backend/update.sh` only *restarts services*; it does NOT sync code. Running `update.sh` alone will silently leave the VM on stale code.

Run from the project root:

```
python3 scripts/deploy_to_vm.py
```

What this does (6-step pipeline, defined in the script):
1. rsync code → `/var/www/backend` on `20.6.132.108` (excludes `.env`, backups, `__pycache__`)
2. Verifies venv at `/var/www/backend/backend/python/venv`
3. Installs/updates Python requirements
4. Checks `.env` exists on VM (DB_PASSWORD, VAULT_MASTER_KEY)
5. Computes restart scope from the deploy manifest
6. Restarts the affected services: `esa-backend`, `backend-orchestrator`, `backend-mcp` (+ reloads nginx if config changed)

Connection (handled by the script via paramiko, you don't need to ssh manually):
- Host: `20.6.132.108`, user: `esa_bk_admin`, key: `~/.ssh/id_ed25519_vm`
- Sudo password: `VM_SSH_PASSWORD` from `.env` (the script reads it itself)

**Do NOT** ssh in and run `update.sh` as a substitute. If you only need a service restart (no code change), say so explicitly and ask the user — don't reach for `update.sh` by default.

Report the deploy_to_vm.py output to the user (the final "Deployment complete!" + service status block).

---

## Step 6: Auto-Document

After a successful deployment (Step 3 push, or Step 5 VM deploy — whichever was the final step), automatically generate documentation for all changed files:

1. Identify what was changed using `git diff --name-only HEAD~1` (the commit just pushed).
2. For each changed file, determine the appropriate document target:
   - `backend/python/web/templates/tools/*.html` → `tool:<name>`
   - `backend/python/web/routes/*.py` → `api:<blueprint>`
   - `backend/python/datalayer/*.py` → `pipeline:<name>`
   - Other modules → `module:<path>`
3. Run the `/document` skill logic for each identified target.
4. If docs already exist for a target, **update** the existing file rather than creating a duplicate.
5. Report what was documented.

This step is non-blocking — if documentation generation fails for any target, log the error and continue with the rest.

---

## Rules
- NEVER commit `.env` or files containing secrets.
- NEVER force push.
- Always confirm with the user before pushing and before deploying.
- If the security gate fails, list all findings and stop — do not offer to skip.
