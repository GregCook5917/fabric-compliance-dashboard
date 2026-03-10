# Fabric Git Governance Scanner — Setup Guide

## What This Does

A Fabric Notebook + Pipeline that runs on a schedule, scans your Dev workspace
for Git compliance violations, and writes results to Azure SQL for Power BI reporting.

**Violations detected:**
- Items modified in Dev but not committed to ADO Git
- Items promoted to Test/Prod while in an uncommitted state
- Items where Fabric's last-modified timestamp is newer than the last ADO commit

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Fabric workspace (Dev) | Must be connected to an ADO Git repo |
| Azure SQL Database | Existing database; script creates schema/tables |
| Fabric Deployment Pipeline | Dev → Test → Prod stages configured |
| ADO repository | The repo connected to your Dev workspace |
| Fabric PAT | User with Workspace Member access or higher |
| ADO PAT | Scopes: Code (Read), Project and Team (Read) |

---

## Step 1 — Create Your PATs

### Fabric PAT
1. Go to `app.fabric.microsoft.com`
2. Click your profile picture (top right) → **Developer settings**
3. Generate a new PAT — set expiry to 90 days (add a calendar reminder to rotate it)
4. Copy it — you won't see it again

### ADO PAT
1. Go to `dev.azure.com/{your-org}`
2. Click your profile picture → **Personal Access Tokens**
3. New Token → Scopes: **Code (Read)** + **Project and Team (Read)**
4. Copy it

---

## Step 2 — Create the Notebook in Fabric

1. Open your **Dev workspace** in Fabric
2. **+ New** → **Notebook**
3. Rename it: `governance_scanner`
4. Paste the contents of `fabric_governance_scanner.py` into the notebook
   - Each `# CELL N` comment block maps to a separate notebook cell
   - Split them using the **+ Code** button between cells
5. Fill in the `CONFIG` dict in Cell 2 with your real values:
   ```python
   CONFIG = {
       "fabric_pat":             "your-fabric-pat",
       "dev_workspace_id":       "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
       "deployment_pipeline_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
       "ado_pat":                "your-ado-pat",
       "ado_organization":       "your-org",
       "ado_project":            "your-project",
       "ado_repository":         "your-repo-name",
       "sql_server":             "yourserver.database.windows.net",
       "sql_database":           "your-database",
       "sql_username":           "your-sql-user",
       "sql_password":           "your-sql-password",
       ...
   }
   ```

### Finding Your Workspace ID
- Open the Dev workspace in Fabric
- Look at the URL: `app.fabric.microsoft.com/groups/{WORKSPACE_ID}/...`

### Finding Your Deployment Pipeline ID
- Option A: Open the pipeline in Fabric → check the URL
- Option B: Run this in a notebook cell:
  ```python
  import requests, base64
  pat = "YOUR_FABRIC_PAT"
  token = base64.b64encode(f":{pat}".encode()).decode()
  r = requests.get(
      "https://api.fabric.microsoft.com/v1/deploymentPipelines",
      headers={"Authorization": f"Basic {token}"}
  )
  print(r.json())
  ```

---

## Step 3 — Run the SQL Setup Scripts

Connect to your Azure SQL Database and run `governance_views.sql`.
This creates:
- `governance` schema
- 4 base tables (created by the notebook on first run)
- 5 views for Power BI

Use Azure Data Studio, SSMS, or the Fabric SQL query editor if your SQL DB
is connected to a Lakehouse.

---

## Step 4 — Test Run the Notebook

1. Run Cell 1 (install deps) — only needed once
2. Run all cells top to bottom
3. Check Cell 7 output — it should list any violations found
4. Check Cell 9 output — should confirm rows loaded to SQL
5. Check Cell 10 — summary JSON output

**Expected output (Cell 10):**
```json
{
  "scan_timestamp": "2026-03-10T09:14:00+00:00",
  "total_items_tracked": 28,
  "uncommitted_items": 5,
  "total_violations": 7,
  "promoted_while_dirty": 3,
  "high_severity": 2,
  "git_sync_rate_pct": 82.1
}
```

---

## Step 5 — Create the Fabric Pipeline

1. In your Dev workspace: **+ New** → **Data Pipeline**
2. Name it: `governance_scanner_pipeline`
3. Add a **Notebook activity**:
   - Settings → Notebook: `governance_scanner`
   - Base parameters: none required (all config is inside the notebook)
4. Add a **Schedule trigger**:
   - Recommended: every 30 minutes or hourly
   - **New trigger** → Recurrence → Repeat every `30 Minutes`
5. Save and **Validate** — fix any warnings
6. Click **Run** to test the full pipeline end-to-end
7. Check the **Run history** tab — the output column will show the summary JSON

---

## Step 6 — Connect Power BI

1. Open Power BI Desktop
2. **Get Data** → **Azure SQL Database**
3. Server: `yourserver.database.windows.net`
   Database: `your-database`
4. Import these views:
   - `governance.vw_violation_summary` → main violations table
   - `governance.vw_git_health_summary` → KPI cards
   - `governance.vw_violations_by_person` → leaderboard
   - `governance.vw_deployment_ops_with_status` → promotion history
   - `governance.vw_item_type_breakdown` → breakdown chart
5. Publish to your Fabric workspace
6. Set the dataset refresh to match the pipeline schedule (every 30 min)

---

## Rotating PATs (Important)

PATs expire. When they do, the pipeline will fail silently on auth errors.

**Recommended:** Store PATs in Azure Key Vault and fetch them in the notebook:
```python
from azure.keyvault.secrets import SecretClient
from azure.identity import ManagedIdentityCredential

credential = ManagedIdentityCredential()
client = SecretClient(vault_url="https://your-vault.vault.azure.net/", credential=credential)
fabric_pat = client.get_secret("fabric-governance-pat").value
```
This requires the Fabric capacity's Managed Identity to have Key Vault `Get` permissions.

For now with PATs: set a calendar reminder 1 week before expiry to rotate them.

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| 401 on Fabric API | Expired or wrong PAT | Regenerate Fabric PAT |
| 401 on ADO API | Expired ADO PAT or missing scope | Regenerate with Code (Read) scope |
| 0 items returned from git/status | Wrong workspace ID | Verify ID from workspace URL |
| 0 deployment operations | Wrong pipeline ID | Use discovery snippet above |
| pyodbc connection error | Firewall rules | Add Azure Function/Fabric IP to SQL firewall; or enable "Allow Azure services" |
| TRUNCATE permission error | SQL user lacks DDL rights | Grant `db_ddladmin` role to SQL user |

---

## Escalation Path (Future State)

Once the team's maturity improves, the natural next steps are:

1. **Block promotions** — Add an ADO Branch Policy requiring linked Work Items before merge
2. **Enforce via ADO Pipeline** — Add the git/status API call as a pipeline gate
3. **Automate Teams alerts** — Webhook from the notebook when violations are found
4. **Historical trending** — Add a `fact_violations_history` table that appends
   (not truncates) so you can trend improvement over time
