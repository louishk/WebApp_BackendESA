# Status

Show a quick status summary of the current session: plan progress, git state, and VM deployment state.

---

## 1. Plan / Task Progress

Check if there is an active plan or task list in this conversation. If yes, show:

- **Current step**: which step we're on
- **Completed**: list completed items with checkmarks
- **Remaining**: list remaining items
- **Blockers**: any known blockers or issues

If there is no active plan or task list, say: "No active plan in this session."

---

## 2. Git Status

Run these commands and summarize:

1. `git status` — show branch name, staged/unstaged/untracked files count
2. `git log --oneline -3` — show last 3 commits
3. Check if local branch is ahead/behind remote: `git rev-list --left-right --count HEAD...@{upstream} 2>/dev/null`

Present as:
- **Branch**: name
- **Uncommitted changes**: X staged, Y modified, Z untracked (or "clean")
- **Unpushed commits**: N commits ahead of remote (or "in sync")
- **Recent commits**: last 3 one-liners

---

## 3. Deploy Status

Based on conversation history, report:

- Whether `/deploy` or `deploy_to_vm.py` was run during this session
- If yes: what was deployed (commit or description) and whether it succeeded
- If no: say "Not deployed this session"

---

## Format

Present everything in a single compact summary like:

```
## Status

### Plan
- Step 3/7: Adding API endpoint for unit search
- Done: schema migration, model updates
- Next: route handler, frontend template

### Git
- Branch: feature/unit-search
- Changes: 2 staged, 1 modified, 0 untracked
- Unpushed: 1 commit ahead
- Recent: abc1234 Add unit search model | def5678 Run migration 025

### Deploy
- Not deployed this session (or: Deployed at <time> — <description> ✓)
```

## Rules
- Keep it concise — no verbose explanations
- Never expose passwords in output
