# Deploy Pipeline: Security Check → Commit → Push → Deploy

Execute a secure deployment pipeline with the following steps. Stop immediately if any gate fails.

Context for this commit: $ARGUMENTS

---

## Step 1: Security Gate — Pentest Review

Use the **pentest-code-reviewer** agent (via the Task tool with `subagent_type: "pentest-code-reviewer"`) to review ALL staged and modified files (use `git diff` and `git diff --cached` to identify them).

The agent must check for CRITICAL, HIGH, and MEDIUM severity vulnerabilities.

### Decision:
- **If any CRITICAL, HIGH, or MEDIUM findings exist**: Report them clearly, do NOT proceed to commit. Stop here.
- **If only LOW/INFO or no findings**: Proceed to Step 2.

---

## Step 2: Commit & Push

1. Stage all relevant changed files (be selective — never stage `.env`, credentials, or secrets).
2. Create a commit with a descriptive message based on the actual changes and the context provided above.
3. Push to `origin` on the current branch.
4. Confirm push succeeded before continuing.

---

## Step 3: Ask About VM Deployment

Ask the user: **"Push successful. Do you want to deploy to the VM now?"**

- If **No** → Stop here. Done.
- If **Yes** → Proceed to Step 4.

---

## Step 4: Deploy to VM

SSH into the production VM using key-based authentication and run the update script.

Connection details:
- Host: `57.158.27.35`
- Port: `22`
- User: `esa_pbi_admin`
- SSH key: `~/.ssh/id_ed25519_vm`
- Password (for sudo only): read `VM_SSH_PASSWORD` from `.env`

Execute in a **single command**:

```
ssh -i ~/.ssh/id_ed25519_vm -o StrictHostKeyChecking=no esa_pbi_admin@57.158.27.35 'sed -i "s/\r$//" /tmp/update.sh && echo <VM_SSH_PASSWORD> | sudo -S bash /tmp/update.sh'
```

Key details:
- Uses SSH key auth (no `sshpass` needed)
- The update script is at `/tmp/update.sh` (NOT `/update.sh`)
- Fix Windows line endings with `sed` before running
- The script requires `sudo` — read `VM_SSH_PASSWORD` from `.env` and pipe via `echo <password> | sudo -S`
- Do this all in one SSH command to avoid multiple round trips

Report the deployment output to the user.

---

## Rules
- NEVER commit `.env` or files containing secrets.
- NEVER force push.
- Always confirm with the user before pushing and before deploying.
- If the security gate fails, list all findings and stop — do not offer to skip.
