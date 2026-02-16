# Security Review & Auto-Remediation Pipeline

Execute a full security audit with automated remediation. This is an iterative process that loops until all critical, high, and medium issues are resolved.

Scope: $ARGUMENTS
If no scope is provided, review all recently modified files (use `git diff --name-only` and `git ls-files --others --exclude-standard`).

---

## Step 1: Security Audit

Use the **pentest-code-reviewer** agent (via the Task tool with `subagent_type: "pentest-code-reviewer"`) to perform a comprehensive security audit on all files in scope.

The agent must classify every finding as: CRITICAL, HIGH, MEDIUM, LOW, or INFO.

---

## Step 2: Findings Report

Present a clear summary table sorted by severity:

```
| # | Severity | File:Line | Vulnerability | CWE |
|---|----------|-----------|---------------|-----|
| 1 | CRITICAL | ...       | ...           | ... |
| 2 | HIGH     | ...       | ...           | ... |
```

Then for each CRITICAL, HIGH, and MEDIUM finding, list:
- What the issue is
- How it can be exploited
- Proposed fix (specific code change)

If there are **no CRITICAL, HIGH, or MEDIUM findings** → report the LOW/INFO items for awareness and STOP. The codebase is clean.

---

## Step 3: Resolution Planning

Use the **web-project-director** agent (via the Task tool with `subagent_type: "web-project-director"`) to:

1. Analyze all CRITICAL, HIGH, and MEDIUM findings from Step 2
2. Group fixes by domain (backend, frontend, API, database, config)
3. Prioritize the order of fixes (dependencies, risk level)
4. Create a resolution plan with concrete tasks for each fix
5. Assign each task to the appropriate agent type:
   - **backend-api-architect** — backend Python code, API routes, data models, server config
   - **frontend-developer** — templates, JavaScript, HTML, CSS, CSP headers
   - **general-purpose** — config files, environment, migrations, anything else

---

## Step 4: Dispatch & Fix

Create a team and execute the resolution plan:

1. Create tasks using TaskCreate for each fix from the plan
2. Spawn the appropriate agents (via the Task tool with the correct `subagent_type`) to work on fixes **in parallel where possible**
3. Each agent must:
   - Read the vulnerable code
   - Apply the specific fix
   - Verify the fix doesn't break existing functionality
4. Wait for all agents to complete their fixes

**Rules for agents:**
- Only modify what is necessary to fix the vulnerability — no refactoring, no feature changes
- Preserve existing code style and patterns
- Never introduce new dependencies unless absolutely required for the fix

---

## Step 5: Verification Sweep

Run the **pentest-code-reviewer** agent again on ALL files that were modified during Step 4.

### Decision:
- **No CRITICAL, HIGH, or MEDIUM findings remaining** → Report success. List any remaining LOW/INFO items for awareness. Pipeline complete.
- **CRITICAL, HIGH, or MEDIUM findings still exist** → Report what remains and what was introduced. **Go back to Step 3** with only the outstanding findings. Maximum 3 iterations to prevent infinite loops.

---

## Iteration Safeguard

Track the iteration count. If after **3 full cycles** there are still CRITICAL/HIGH/MEDIUM issues:
- List all remaining findings
- Explain why they could not be auto-resolved
- Ask the user how to proceed

---

## Rules
- NEVER modify `.env` or files containing secrets
- NEVER remove security features to "fix" a finding
- NEVER introduce new vulnerabilities while fixing existing ones
- Keep fixes minimal and surgical
- Report everything transparently — no hiding of findings
