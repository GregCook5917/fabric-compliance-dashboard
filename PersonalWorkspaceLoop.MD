# Personal Workspace — Git Integration & Feature Branch Workflow

Use a personal Fabric workspace as your sandbox. Work freely, commit to a feature branch, raise a PR into main, and the Dev workspace picks it up automatically. Nothing reaches Dev without a code review.

---

## One-time setup

**1. Create your personal workspace**
- Fabric portal → Workspaces → **+ New workspace**
- Name it `[YourName]-Dev` (e.g. `JSmith-Dev`)
- Assign it to the same Fabric capacity as the team's Dev workspace

**2. Create a feature branch in ADO**
- Repos → Branches → **+ New branch**
- Always base it on `main`
- Follow the naming convention: `feature/<ticket-id>-<short-description>`
  - e.g. `feature/JIRA-1042-monthly-close-report`

**3. Connect your workspace to git**
- Workspace settings → **Git integration** → Azure DevOps
- Select the same repo used by Dev, and point it at your feature branch
- Click **Connect and sync** — your workspace will load whatever is currently on that branch

---

## Day-to-day workflow

1. **Start of session** — check Source control. If the branch has been updated, click **Update all** before touching anything.
2. **Build** — create and edit reports, datasets, notebooks, pipelines as normal in your personal workspace.
3. **Commit** — Source control → select items → write a meaningful commit message → **Commit**
   - ✅ `JIRA-1042: Add monthly close report with GL breakdown by cost centre`
   - ❌ `updates` / `wip` / `fixed it`
4. **Keep current** — if other PRs merge to main while you're working, merge main into your feature branch in ADO, then **Update all** in Fabric.
5. **Raise a PR** — ADO → Pull Requests → **New PR**. Source: your feature branch. Target: `main`. Add a reviewer — self-merging is not allowed.
6. **After merge** — the Dev workspace auto-syncs with main. Your changes are now ready for the deployment pipeline. Delete your feature branch and start fresh for the next ticket.

---

## Before raising your PR

Make sure all of these are true:

- [ ] All workspace items show **Synced** (no uncommitted changes)
- [ ] Branch is up to date with main (no conflicts)
- [ ] Changes have been tested in your personal workspace
- [ ] PR description includes a summary, testing steps, and a link to the ticket
- [ ] A reviewer is assigned

---

## Git status quick reference

| Status | What it means |
|---|---|
| **Synced** | Workspace matches the branch — nothing to do |
| **Uncommitted changes** | Modified locally, not yet committed |
| **Conflict** | Branch was updated externally — resolve before committing |
| **Not in git** | New item that has never been committed — won't be in the PR until you commit it |

---

## Why this matters

Everything in Dev must come through a PR. This is what keeps the compliance checks clean:

- **Unsynced violations** — can't happen because you never edit directly in Dev
- **Uncommitted promotions** — can't happen because Dev only updates after a merge to main
- **Orphaned objects** — can't happen because nothing reaches Test/Prod except through the deployment pipeline

---

## Common issues

**Can't connect to ADO?**
Check that git integration is enabled in the Fabric Admin portal (Tenant settings → Integration settings) and that you have Contributor access to the repo.

**Conflict after updating?**
Open Source control, click the conflicted item, and choose which version to keep. If unsure, ask your team lead before resolving.

**Dev didn't sync after your PR merged?**
Wait a few minutes and refresh. If it still hasn't updated, a workspace admin can trigger it manually via Workspace settings → Git integration → Update all.