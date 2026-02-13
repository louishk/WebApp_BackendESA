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

SSH into the production VM and run the update script. Connection details are in `.env`:
- Host: `VM_SSH_HOST`
- Port: `VM_SSH_PORT`
- User: `VM_SSH_ROOT_USERNAME`
- Password: `VM_SSH_PASSWORD`

Read the `.env` file to get the actual values, then execute:

```
sshpass -p '<password>' ssh -o StrictHostKeyChecking=no -p <port> <user>@<host> 'bash /update.sh'
```

Report the deployment output to the user.

---

## Rules
- NEVER commit `.env` or files containing secrets.
- NEVER force push.
- Always confirm with the user before pushing and before deploying.
- If the security gate fails, list all findings and stop — do not offer to skip.
