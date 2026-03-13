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

SSH into the production VM using key-based authentication and run the update script.

Connection details:
- Host: `20.6.132.108`
- Port: `22`
- User: `esa_bk_admin`
- SSH key: `~/.ssh/id_ed25519_vm`
- Password (for sudo only): read `VM_SSH_PASSWORD` from `.env`

Execute in a **single command**:

```
ssh -i ~/.ssh/id_ed25519_vm -o StrictHostKeyChecking=no esa_bk_admin@20.6.132.108 'echo <VM_SSH_PASSWORD> | sudo -S sed -i "s/\r$//" /var/www/backend/update.sh && echo <VM_SSH_PASSWORD> | sudo -S bash /var/www/backend/update.sh'
```

Key details:
- Uses SSH key auth (no `sshpass` needed)
- The update script is at `/var/www/backend/update.sh` (owned by root, requires sudo)
- Fix Windows line endings with `sudo sed` before running (directory is root-owned)
- The script requires `sudo` — read `VM_SSH_PASSWORD` from `.env` and pipe via `echo <password> | sudo -S`
- Do this all in one SSH command to avoid multiple round trips

Report the deployment output to the user.

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
