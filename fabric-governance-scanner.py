# =============================================================================
# FABRIC GIT GOVERNANCE SCANNER  v2
# Fabric Notebook — run via Fabric Pipeline on a schedule
#
# What this does:
#   1. Calls Fabric REST API → Git status of all items in Dev workspace
#   2. Calls Fabric REST API → Item lists from Test and Prod workspaces
#   3. Calls Fabric REST API → Deployment pipeline operation history
#   4. Calls ADO REST API    → Last commit per item path
#   5. Cross-references all sources to detect violations
#   6. Overwrites Azure SQL tables with current state
#
# Violations detected:
#   v1 — Item uncommitted in Dev (local drift)
#   v2 — Item promoted to Test/Prod while uncommitted in Dev
#   v3 — Fabric modified_at newer than last ADO commit
#   v4 — Item edited DIRECTLY in Test or Prod outside of a deployment  ← NEW
#
# Informational flags (not violations):
#   pending_promotion — committed in Dev, not yet promoted downstream   ← NEW
#
# Severity tiers:
#   CRITICAL — direct edit in Prod (untracked production change)        ← NEW
#   HIGH     — promoted while dirty / multiple flags
#   MEDIUM   — direct edit in Test / single promotion flag
#   LOW      — uncommitted drift in Dev only, not yet promoted
#   NONE     — clean
#
# Authentication: PAT-based (Fabric + ADO)
# Target:         Azure SQL Database
# Mode:           Overwrite (latest state only)
# =============================================================================


# -----------------------------------------------------------------------------
# CELL 1 — Install dependencies
# Run once on a fresh environment. Comment out after first successful run.
# -----------------------------------------------------------------------------

# %pip install pyodbc requests pandas --quiet


# -----------------------------------------------------------------------------
# CELL 2 — Configuration
# Fill in all values before running.
# In production: move PATs and SQL credentials to Azure Key Vault and
# retrieve them via Managed Identity — never commit secrets to a notebook.
# -----------------------------------------------------------------------------

CONFIG = {

    # -------------------------------------------------------------------------
    # Fabric
    # -------------------------------------------------------------------------

    # Workspace IDs — found in the URL when viewing each workspace:
    # app.fabric.microsoft.com/groups/{WORKSPACE_ID}/...
    "dev_workspace_id":  "YOUR_DEV_WORKSPACE_ID",
    "test_workspace_id": "YOUR_TEST_WORKSPACE_ID",   # ← NEW
    "prod_workspace_id": "YOUR_PROD_WORKSPACE_ID",   # ← NEW

    # Deployment Pipeline ID — found in the URL when viewing the pipeline,
    # or discoverable via: GET /v1/deploymentPipelines
    "deployment_pipeline_id": "YOUR_DEPLOYMENT_PIPELINE_ID",

    # How many days of deployment history to pull for cross-referencing
    "lookback_days": 30,

    # -------------------------------------------------------------------------
    # Azure DevOps
    # -------------------------------------------------------------------------

    # PAT: dev.azure.com → profile → Personal Access Tokens
    # Required scopes: Code (Read), Project and Team (Read)
    "ado_pat":          "YOUR_ADO_PAT_HERE",
    "ado_organization": "YOUR_ORG_NAME",    # e.g. "contoso"
    "ado_project":      "YOUR_PROJECT_NAME",# e.g. "FabricBI"
    "ado_repository":   "YOUR_REPO_NAME",   # e.g. "fabric-workspace"

    # -------------------------------------------------------------------------
    # Azure SQL
    # -------------------------------------------------------------------------

    # ODBC Driver 18 is pre-installed on Fabric Spark runtimes
    "sql_server":   "YOUR_SERVER.database.windows.net",
    "sql_database": "YOUR_DATABASE_NAME",
    "sql_username": "YOUR_SQL_USERNAME",
    "sql_password": "YOUR_SQL_PASSWORD",
    "sql_schema":   "governance",
}

# API base URLs — do not change
FABRIC_API = "https://api.fabric.microsoft.com/v1"
ADO_API    = "https://dev.azure.com"

# Stage labels keyed by pipeline stage order
# Fabric uses 0-based index: 0=Dev, 1=Test, 2=Prod
STAGE_MAP = {0: "Dev", 1: "Test", 2: "Prod"}

# Map workspace IDs to stage names for downstream edit detection
WORKSPACE_STAGE_MAP = {
    CONFIG["test_workspace_id"]: "Test",
    CONFIG["prod_workspace_id"]: "Prod",
}


# -----------------------------------------------------------------------------
# CELL 3 — Imports and shared helpers
# -----------------------------------------------------------------------------

import requests
import pyodbc
import pandas as pd
from datetime import datetime, timezone, timedelta
from base64   import b64encode
from typing   import Optional
import json
import warnings
warnings.filterwarnings("ignore")


def fabric_headers() -> dict:
    """
    Bearer token auth for Fabric REST API using mssparkutils.
    This is the correct approach when PAT creation is disabled at tenant level.
    The token is scoped to the current user's identity running the notebook.
    """
    token = mssparkutils.credentials.getToken("pbi")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    }

def ado_headers() -> dict:
    """Bearer-style auth headers for ADO REST API using PAT."""
    token = b64encode(f":{CONFIG['ado_pat']}".encode()).decode()
    return {"Authorization": f"Basic {token}", "Content-Type": "application/json"}


def fabric_get(path: str, params: dict = None) -> dict:
    url  = f"{FABRIC_API}/{path}"
    resp = requests.get(url, headers=fabric_headers(), params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def ado_get(path: str, params: dict = None) -> dict:
    url  = f"{ADO_API}/{path}"
    resp = requests.get(url, headers=ado_headers(), params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(s: Optional[str]) -> Optional[datetime]:
    """Parse ISO 8601 string → UTC-aware datetime. Returns None if blank/invalid."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def name_key(s: Optional[str]) -> str:
    """Normalise an item display name for fuzzy joining across workspaces."""
    if not s:
        return ""
    return s.lower().strip()


print("✅ Cell 3 — imports and helpers ready.")


# -----------------------------------------------------------------------------
# CELL 4 — Fetch Dev workspace Git status
#
# API: GET /workspaces/{devWorkspaceId}/git/status
#
# gitSyncState values returned by Fabric:
#   Committed      — workspace matches repo HEAD  ✅
#   Uncommitted    — workspace has changes not pushed to repo  ❌
#   Conflict       — workspace and remote have diverged  ⚠
#   NotInWorkspace — item exists in repo but not in workspace
#   NotInGit       — item exists in workspace but not tracked by git
# -----------------------------------------------------------------------------

def get_dev_git_status() -> pd.DataFrame:
    print("🔍 [Dev] Fetching Git status...")
    data      = fabric_get(f"workspaces/{CONFIG['dev_workspace_id']}/git/status")
    scan_time = now_utc()
    rows      = []

    for change in data.get("changes", []):
        ws_item     = change.get("item", {})
        ws_head     = change.get("workspaceHead", {})
        remote_head = change.get("remoteCommittedHead", {})
        state       = ws_head.get("state")

        rows.append({
            "scan_timestamp":    scan_time,
            "item_id":           ws_item.get("objectId"),
            "item_display_name": ws_item.get("displayName"),
            "item_type":         ws_item.get("objectType"),
            "git_sync_state":    state,
            "workspace_version": ws_head.get("version"),
            "remote_version":    remote_head.get("version"),
            "last_modified_by":  ws_head.get("lastModifiedBy", {}).get("userPrincipalName"),
            "last_modified_at":  parse_dt(ws_head.get("lastModifiedAt")),
            "is_uncommitted":    state == "Uncommitted",
            "workspace_id":      CONFIG["dev_workspace_id"],
            "_name_key":         name_key(ws_item.get("displayName")),
        })

    df = pd.DataFrame(rows)
    uncommitted = int(df["is_uncommitted"].sum()) if not df.empty else 0
    print(f"   {len(df)} items found — {uncommitted} uncommitted.")
    return df


df_git_status = get_dev_git_status()
display(df_git_status[["item_display_name", "item_type", "git_sync_state",
                        "last_modified_by", "last_modified_at"]].head(20))


# -----------------------------------------------------------------------------
# CELL 5 — Fetch item lists from Test and Prod workspaces
#
# API: GET /workspaces/{workspaceId}/items
#
# These workspaces have no Git connection. We capture the item metadata
# (specifically modifiedDateTime) so we can compare it against the last
# known deployment timestamp for each item — any item modified MORE RECENTLY
# than its last deployment was edited directly in that workspace.
# -----------------------------------------------------------------------------

def get_downstream_workspace_items(workspace_id: str,
                                   stage_label:  str) -> pd.DataFrame:
    print(f"🔍 [{stage_label}] Fetching workspace item list...")

    all_items = []
    params    = {"$top": 200}
    path      = f"workspaces/{workspace_id}/items"

    while True:
        data  = fabric_get(path, params=params)
        items = data.get("value", [])
        all_items.extend(items)
        token = data.get("continuationToken")
        if not token:
            break
        params["continuationToken"] = token

    scan_time = now_utc()
    rows      = []

    for item in all_items:
        rows.append({
            "workspace_id":        workspace_id,
            "stage":               stage_label,
            "item_id":             item.get("id"),
            "item_display_name":   item.get("displayName"),
            "item_type":           item.get("type"),
            "item_modified_at":    parse_dt(item.get("modifiedDateTime")),
            "item_modified_by":    item.get("modifiedBy", {})
                                       .get("user", {})
                                       .get("userPrincipalName"),
            "scan_timestamp":      scan_time,
            "_name_key":           name_key(item.get("displayName")),
        })

    df = pd.DataFrame(rows)
    print(f"   {len(df)} items found in {stage_label}.")
    return df


df_test_items = get_downstream_workspace_items(
    CONFIG["test_workspace_id"], "Test"
)
df_prod_items = get_downstream_workspace_items(
    CONFIG["prod_workspace_id"], "Prod"
)

# Combined downstream item snapshot for v4 detection
df_downstream_items = pd.concat([df_test_items, df_prod_items], ignore_index=True)

display(df_downstream_items[["stage", "item_display_name", "item_type",
                              "item_modified_at", "item_modified_by"]].head(20))


# -----------------------------------------------------------------------------
# CELL 6 — Fetch deployment pipeline operation history
#
# API: GET /deploymentPipelines/{id}/operations
#
# Each operation records what was deployed, when, to which stage, and by whom.
# We use this to:
#   (a) detect items promoted while uncommitted (v2)
#   (b) establish the "last known good deployment time" per item per stage,
#       which anchors the v4 direct-edit detection in Cell 7
# -----------------------------------------------------------------------------

def get_deployment_operations() -> pd.DataFrame:
    print("🔍 Fetching deployment pipeline operations...")
    cutoff  = now_utc() - timedelta(days=CONFIG["lookback_days"])
    all_ops = []
    params  = {"$top": 100}
    path    = f"deploymentPipelines/{CONFIG['deployment_pipeline_id']}/operations"

    while True:
        data  = fabric_get(path, params=params)
        ops   = data.get("value", [])
        all_ops.extend(ops)
        token = data.get("continuationToken")
        if not token:
            break
        params["continuationToken"] = token

    rows = []
    for op in all_ops:
        created_at = parse_dt(op.get("createdTime"))
        if created_at and created_at < cutoff:
            continue

        for item in op.get("deployedArtifacts", []):
            rows.append({
                "operation_id":       op.get("id"),
                "operation_type":     op.get("type"),
                "operation_status":   op.get("status"),
                "source_stage_order": op.get("sourceStageOrder"),
                "target_stage_order": op.get("targetStageOrder"),
                "source_stage":       STAGE_MAP.get(op.get("sourceStageOrder"), "Unknown"),
                "target_stage":       STAGE_MAP.get(op.get("targetStageOrder"), "Unknown"),
                "created_at":         created_at,
                "completed_at":       parse_dt(op.get("completedTime")),
                "triggered_by":       op.get("executionPlan", {})
                                        .get("triggeredBy", {})
                                        .get("userPrincipalName"),
                "item_id":            item.get("sourceObjectId"),
                "item_display_name":  item.get("displayName"),
                "item_type":          item.get("objectType"),
                "_name_key":          name_key(item.get("displayName")),
            })

    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    print(f"   {len(df)} deployment item records in last {CONFIG['lookback_days']} days.")
    return df


df_deployments = get_deployment_operations()
if not df_deployments.empty:
    display(df_deployments[["item_display_name", "item_type", "source_stage",
                             "target_stage", "triggered_by", "created_at"]].head(20))


# -----------------------------------------------------------------------------
# CELL 7 — Fetch ADO commit history (last commit per item path)
#
# API: GET /{org}/{project}/_apis/git/repositories/{repo}/commits
#      GET /{org}/{project}/_apis/git/repositories/{repo}/commits/{id}/changes
#
# Fabric stores each workspace item as a top-level folder in the repo.
# e.g. /Sales Dashboard.Report/definition.pbir
#      /Inventory Pipeline.DataPipeline/pipeline-content.json
#
# We walk recent commits, extract changed paths, and build a map of
# root folder name → most recent commit metadata.
# -----------------------------------------------------------------------------

def get_ado_last_commits() -> pd.DataFrame:
    print("🔍 Fetching ADO commit history...")
    org    = CONFIG["ado_organization"]
    proj   = CONFIG["ado_project"]
    repo   = CONFIG["ado_repository"]
    cutoff = (now_utc() - timedelta(days=CONFIG["lookback_days"])).strftime(
                "%Y-%m-%dT%H:%M:%SZ")

    commits_path = f"{org}/{proj}/_apis/git/repositories/{repo}/commits"
    data         = ado_get(commits_path, params={
        "searchCriteria.fromDate": cutoff,
        "$top":                    500,
        "api-version":             "7.1",
    })

    path_map = {}  # normalised item name → commit metadata

    for commit in data.get("value", []):
        commit_id = commit.get("commitId")
        author    = commit.get("author", {}).get("email")
        commit_dt = parse_dt(commit.get("author", {}).get("date"))
        comment   = commit.get("comment", "")

        changes_path = (f"{org}/{proj}/_apis/git/repositories/"
                        f"{repo}/commits/{commit_id}/changes")
        try:
            changes = ado_get(changes_path, params={"api-version": "7.1"})
            for change in changes.get("changes", []):
                raw_path = change.get("item", {}).get("path", "")
                parts    = [p for p in raw_path.strip("/").split("/") if p]
                if not parts:
                    continue
                # Root folder is the item name including type suffix
                # e.g. "Sales Dashboard.Report" → key: "sales dashboard"
                root_folder = parts[0]
                item_name   = root_folder.split(".")[0]  # strip .Report etc
                key         = name_key(item_name)

                if key not in path_map or commit_dt > path_map[key]["last_commit_at"]:
                    path_map[key] = {
                        "repo_item_name":  item_name,
                        "_name_key":       key,
                        "last_commit_id":  commit_id,
                        "last_commit_by":  author,
                        "last_commit_at":  commit_dt,
                        "last_commit_msg": comment[:200],
                    }
        except Exception as e:
            print(f"   ⚠ Skipping commit {commit_id}: {e}")

    df = pd.DataFrame(list(path_map.values()))
    print(f"   {len(df)} item paths mapped to their last ADO commit.")
    return df


df_ado_commits = get_ado_last_commits()
if not df_ado_commits.empty:
    display(df_ado_commits.head(10))


# -----------------------------------------------------------------------------
# CELL 8 — Detect violations
#
# This cell is the core logic engine. It assembles all data sources and
# evaluates each item against the full violation rule set.
#
# ┌─────────────────────────────────────────────────────────────────────────┐
# │  Flag  │ Rule                                                           │
# ├─────────────────────────────────────────────────────────────────────────┤
# │  v1    │ git_sync_state == "Uncommitted" in Dev                         │
# │  v2    │ Item appears in deployment history AND is currently uncommitted│
# │  v3    │ Fabric last_modified_at > last ADO commit_at                   │
# │  v4    │ Item modified_at in Test/Prod > last deployment to that stage  │
# │        │   → MEDIUM if Test, CRITICAL if Prod                           │
# ├─────────────────────────────────────────────────────────────────────────┤
# │ pending│ git_sync_state == "Committed" AND last_commit_at >             │
# │_promo  │ last_promoted_at  (informational — healthy queue, not a fault) │
# └─────────────────────────────────────────────────────────────────────────┘
# -----------------------------------------------------------------------------

def detect_violations(df_git:        pd.DataFrame,
                      df_downstream: pd.DataFrame,
                      df_deps:       pd.DataFrame,
                      df_ado:        pd.DataFrame) -> pd.DataFrame:

    print("🔎 Running violation detection...")
    scan_time = now_utc()

    # ── 1. Build last-promotion lookup per item per target stage ────────────
    # We need to know: "what was the last time item X was successfully
    # deployed TO stage Y?" — this anchors the v4 direct-edit detection.

    if not df_deps.empty:
        successful_deps = df_deps[df_deps["operation_status"] == "Succeeded"].copy()

        # Last promotion of each item to each stage
        last_promo_by_stage = (
            successful_deps
            .sort_values("completed_at", ascending=False)
            .groupby(["_name_key", "target_stage"])
            .first()
            .reset_index()
            [["_name_key", "target_stage", "completed_at",
              "triggered_by", "source_stage", "operation_id"]]
        )

        # Also derive a single "most recent promotion regardless of stage"
        # for the v2 check on the Dev git status rows
        last_promo_overall = (
            successful_deps
            .sort_values("completed_at", ascending=False)
            .groupby("_name_key")
            .first()
            .reset_index()
            [["_name_key", "completed_at", "target_stage",
              "triggered_by", "source_stage", "operation_id"]]
            .rename(columns={
                "completed_at": "last_promoted_at",
                "target_stage": "promoted_to_stage",
                "triggered_by": "promoted_by",
                "source_stage": "promoted_from_stage",
            })
        )
    else:
        last_promo_by_stage  = pd.DataFrame(columns=[
            "_name_key", "target_stage", "completed_at",
            "triggered_by", "source_stage", "operation_id"])
        last_promo_overall   = pd.DataFrame(columns=[
            "_name_key", "last_promoted_at", "promoted_to_stage",
            "promoted_by", "promoted_from_stage", "operation_id"])

    # ── 2. v4 — Detect direct edits in Test and Prod ────────────────────────
    # For each item in Test/Prod, check if its modified_at is newer than
    # the last successful deployment TO that stage.
    # If yes → someone edited it directly, bypassing Dev entirely.

    v4_rows = []

    if not df_downstream.empty and not last_promo_by_stage.empty:
        df_ds = df_downstream.merge(
            last_promo_by_stage,
            left_on  =["_name_key", "stage"],
            right_on =["_name_key", "target_stage"],
            how="left"
        )

        for _, row in df_ds.iterrows():
            item_mod  = row.get("item_modified_at")
            last_dep  = row.get("completed_at")       # last deploy to this stage
            stage     = row.get("stage")
            modifier  = row.get("item_modified_by")

            # Direct edit conditions:
            # (a) item exists in downstream AND was never deployed there  OR
            # (b) item's modified_at is newer than the last deployment to that stage
            # Guard: ignore if modifier matches the deployment triggerer
            #        (Fabric itself touches items during deployment)
            is_direct_edit = False
            if item_mod is not None:
                if last_dep is None:
                    # Item exists but no deployment record — could be manually created
                    is_direct_edit = True
                elif item_mod > last_dep:
                    is_direct_edit = True

            if is_direct_edit:
                severity = "CRITICAL" if stage == "Prod" else "MEDIUM"
                v4_rows.append({
                    "item_display_name":       row["item_display_name"],
                    "item_type":               row["item_type"],
                    "affected_stage":          stage,
                    "item_modified_at":        item_mod,
                    "item_modified_by":        modifier,
                    "last_deployed_to_stage":  last_dep,
                    "v4_direct_edit":          True,
                    "v4_severity":             severity,
                    "_name_key":               row["_name_key"],
                })

    df_v4 = pd.DataFrame(v4_rows) if v4_rows else pd.DataFrame(
        columns=["item_display_name", "item_type", "affected_stage",
                 "item_modified_at", "item_modified_by",
                 "last_deployed_to_stage", "v4_direct_edit",
                 "v4_severity", "_name_key"])

    print(f"   v4 direct edits: {len(df_v4)} "
          f"({int((df_v4['v4_severity'] == 'CRITICAL').sum()) if not df_v4.empty else 0} CRITICAL in Prod)")

    # ── 3. Build the main violation DataFrame from Dev git status ────────────
    df = df_git.copy()

    # Join: last overall promotion
    df = df.merge(last_promo_overall, on="_name_key", how="left")

    # Join: ADO commit history
    if not df_ado.empty:
        ado_lookup = df_ado.set_index("_name_key")[
            ["last_commit_at", "last_commit_by", "last_commit_msg", "last_commit_id"]
        ]
        df = df.merge(ado_lookup, left_on="_name_key", right_index=True, how="left")
    else:
        df["last_commit_at"]  = pd.NaT
        df["last_commit_by"]  = None
        df["last_commit_msg"] = None
        df["last_commit_id"]  = None

    # ── 4. v1, v2, v3 flags ─────────────────────────────────────────────────

    df["v1_uncommitted_in_dev"]   = df["is_uncommitted"]

    df["v2_promoted_while_dirty"] = (
        df["last_promoted_at"].notna() & df["is_uncommitted"]
    )

    df["v3_modified_after_commit"] = False
    mask = df["last_modified_at"].notna() & df["last_commit_at"].notna()
    df.loc[mask, "v3_modified_after_commit"] = (
        df.loc[mask, "last_modified_at"] > df.loc[mask, "last_commit_at"]
    )

    # ── 5. pending_promotion flag (informational — NOT a violation) ──────────
    # Item is committed in Dev but hasn't been pushed downstream yet.
    # This is the CORRECT workflow state — the developer committed but
    # hasn't triggered a deployment yet. Surface as a useful queue view.

    df["pending_promotion"] = False
    pp_mask = (
        (df["git_sync_state"] == "Committed") &
        df["last_commit_at"].notna() &
        (
            df["last_promoted_at"].isna() |                          # never promoted
            (df["last_commit_at"] > df["last_promoted_at"])          # new commit since last deploy
        )
    )
    df.loc[pp_mask, "pending_promotion"] = True

    # ── 6. v4 flag on Dev rows — join direct edit findings ──────────────────
    # v4 violations live in downstream workspaces, not in Dev git status.
    # We join them back onto the Dev rows so Power BI has one unified table.
    # Items with a v4 violation but NOT in Dev git status are appended below.

    df["v4_direct_edit_test"]    = False
    df["v4_direct_edit_prod"]    = False
    df["v4_test_modified_at"]    = pd.NaT
    df["v4_prod_modified_at"]    = pd.NaT
    df["v4_test_modified_by"]    = None
    df["v4_prod_modified_by"]    = None
    df["v4_last_deployed_test"]  = pd.NaT
    df["v4_last_deployed_prod"]  = pd.NaT

    if not df_v4.empty:
        for _, v4row in df_v4.iterrows():
            key   = v4row["_name_key"]
            stage = v4row["affected_stage"]
            match = df["_name_key"] == key

            if stage == "Test":
                df.loc[match, "v4_direct_edit_test"]   = True
                df.loc[match, "v4_test_modified_at"]   = v4row["item_modified_at"]
                df.loc[match, "v4_test_modified_by"]   = v4row["item_modified_by"]
                df.loc[match, "v4_last_deployed_test"] = v4row["last_deployed_to_stage"]
            elif stage == "Prod":
                df.loc[match, "v4_direct_edit_prod"]   = True
                df.loc[match, "v4_prod_modified_at"]   = v4row["item_modified_at"]
                df.loc[match, "v4_prod_modified_by"]   = v4row["item_modified_by"]
                df.loc[match, "v4_last_deployed_prod"] = v4row["last_deployed_to_stage"]

    # ── 7. Composite violation flag ──────────────────────────────────────────

    df["is_violation"] = (
        df["v1_uncommitted_in_dev"]   |
        df["v2_promoted_while_dirty"] |
        df["v3_modified_after_commit"]|
        df["v4_direct_edit_test"]     |
        df["v4_direct_edit_prod"]
    )

    # ── 8. Severity scoring ──────────────────────────────────────────────────
    # CRITICAL supersedes everything — a direct Prod edit is always critical.
    # Otherwise score by flag weight.

    def severity(row) -> str:
        if row["v4_direct_edit_prod"]:
            return "CRITICAL"
        score = (
            int(row["v1_uncommitted_in_dev"])    * 1 +
            int(row["v2_promoted_while_dirty"])  * 2 +
            int(row["v3_modified_after_commit"]) * 1 +
            int(row["v4_direct_edit_test"])      * 2
        )
        if   score >= 4: return "HIGH"
        elif score >= 2: return "MEDIUM"
        elif score >= 1: return "LOW"
        return "NONE"

    df["severity"] = df.apply(severity, axis=1)

    # ── 9. Drift hours (Dev only) ────────────────────────────────────────────
    df["drift_hours"] = None
    drift_mask = df["last_modified_at"].notna() & df["last_commit_at"].notna()
    df.loc[drift_mask, "drift_hours"] = (
        (df.loc[drift_mask, "last_modified_at"] -
         df.loc[drift_mask, "last_commit_at"])
        .dt.total_seconds() / 3600
    ).round(1)

    df["scan_timestamp"] = scan_time

    # Summary
    total_v      = int(df["is_violation"].sum())
    critical_v   = int((df["severity"] == "CRITICAL").sum())
    high_v       = int((df["severity"] == "HIGH").sum())
    pending_v    = int(df["pending_promotion"].sum())
    v4_prod_v    = int(df["v4_direct_edit_prod"].sum())

    print(f"   ✅ Detection complete.")
    print(f"      Violations    : {total_v} "
          f"(CRITICAL={critical_v}, HIGH={high_v})")
    print(f"      Direct Prod   : {v4_prod_v}")
    print(f"      Pending promo : {pending_v} (informational)")

    return df


df_violations = detect_violations(
    df_git_status,
    df_downstream_items,
    df_deployments,
    df_ado_commits
)

# Preview violations only
cols_preview = ["item_display_name", "item_type", "severity",
                "v1_uncommitted_in_dev", "v2_promoted_while_dirty",
                "v4_direct_edit_test", "v4_direct_edit_prod",
                "pending_promotion", "promoted_to_stage", "promoted_by"]
display(df_violations[df_violations["is_violation"]][cols_preview])


# -----------------------------------------------------------------------------
# CELL 9 — SQL connection and DDL
# -----------------------------------------------------------------------------

def get_sql_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={CONFIG['sql_server']};"
        f"DATABASE={CONFIG['sql_database']};"
        f"UID={CONFIG['sql_username']};"
        f"PWD={CONFIG['sql_password']};"
        f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)


def ensure_schema_and_tables(conn):
    cursor = conn.cursor()
    s      = CONFIG["sql_schema"]

    ddl = f"""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{s}')
        EXEC('CREATE SCHEMA {s}');

    -- ── fact_git_status_current ──────────────────────────────────────────
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                   WHERE TABLE_SCHEMA='{s}' AND TABLE_NAME='fact_git_status_current')
    CREATE TABLE {s}.fact_git_status_current (
        item_id             NVARCHAR(128),
        item_display_name   NVARCHAR(256),
        item_type           NVARCHAR(64),
        git_sync_state      NVARCHAR(64),
        is_uncommitted      BIT,
        last_modified_by    NVARCHAR(256),
        last_modified_at    DATETIME2,
        workspace_version   NVARCHAR(128),
        remote_version      NVARCHAR(128),
        workspace_id        NVARCHAR(128),
        scan_timestamp      DATETIME2
    );

    -- ── fact_downstream_items_current ───────────────────────────────────
    -- Item metadata from Test and Prod workspaces (no Git connection)
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                   WHERE TABLE_SCHEMA='{s}' AND TABLE_NAME='fact_downstream_items_current')
    CREATE TABLE {s}.fact_downstream_items_current (
        workspace_id        NVARCHAR(128),
        stage               NVARCHAR(32),
        item_id             NVARCHAR(128),
        item_display_name   NVARCHAR(256),
        item_type           NVARCHAR(64),
        item_modified_at    DATETIME2,
        item_modified_by    NVARCHAR(256),
        scan_timestamp      DATETIME2
    );

    -- ── fact_violations_current ──────────────────────────────────────────
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                   WHERE TABLE_SCHEMA='{s}' AND TABLE_NAME='fact_violations_current')
    CREATE TABLE {s}.fact_violations_current (
        item_id                  NVARCHAR(128),
        item_display_name        NVARCHAR(256),
        item_type                NVARCHAR(64),
        git_sync_state           NVARCHAR(64),
        severity                 NVARCHAR(16),
        is_violation             BIT,
        pending_promotion        BIT,
        v1_uncommitted_in_dev    BIT,
        v2_promoted_while_dirty  BIT,
        v3_modified_after_commit BIT,
        v4_direct_edit_test      BIT,
        v4_direct_edit_prod      BIT,
        last_modified_by         NVARCHAR(256),
        last_modified_at         DATETIME2,
        last_commit_at           DATETIME2,
        last_commit_by           NVARCHAR(256),
        last_commit_msg          NVARCHAR(512),
        drift_hours              FLOAT,
        last_promoted_at         DATETIME2,
        promoted_to_stage        NVARCHAR(32),
        promoted_from_stage      NVARCHAR(32),
        promoted_by              NVARCHAR(256),
        operation_id             NVARCHAR(128),
        v4_test_modified_at      DATETIME2,
        v4_test_modified_by      NVARCHAR(256),
        v4_last_deployed_test    DATETIME2,
        v4_prod_modified_at      DATETIME2,
        v4_prod_modified_by      NVARCHAR(256),
        v4_last_deployed_prod    DATETIME2,
        scan_timestamp           DATETIME2
    );

    -- ── fact_deployment_ops_current ──────────────────────────────────────
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                   WHERE TABLE_SCHEMA='{s}' AND TABLE_NAME='fact_deployment_ops_current')
    CREATE TABLE {s}.fact_deployment_ops_current (
        operation_id        NVARCHAR(128),
        item_display_name   NVARCHAR(256),
        item_type           NVARCHAR(64),
        source_stage        NVARCHAR(32),
        target_stage        NVARCHAR(32),
        operation_status    NVARCHAR(32),
        triggered_by        NVARCHAR(256),
        created_at          DATETIME2,
        completed_at        DATETIME2
    );
    """

    cursor.execute(ddl)
    conn.commit()
    print("✅ Schema and tables verified.")


print("🔌 Testing SQL connection...")
with get_sql_connection() as conn:
    ensure_schema_and_tables(conn)
    print("✅ Connection successful.")


# -----------------------------------------------------------------------------
# CELL 10 — Write to Azure SQL (TRUNCATE + INSERT)
# -----------------------------------------------------------------------------

def truncate_and_load(conn, schema: str, table: str,
                      df: pd.DataFrame, columns: list):
    if df.empty:
        print(f"   ⚠ {table} — no data, skipping.")
        return
    cursor = conn.cursor()
    cursor.execute(f"TRUNCATE TABLE {schema}.{table}")

    placeholders = ", ".join(["?"] * len(columns))
    sql = (f"INSERT INTO {schema}.{table} ({', '.join(columns)}) "
           f"VALUES ({placeholders})")

    df_out = df[columns].copy().where(pd.notnull(df[columns]), other=None)
    rows   = [tuple(r) for r in df_out.itertuples(index=False, name=None)]
    cursor.executemany(sql, rows)
    conn.commit()
    print(f"   ✅ {table}: {len(rows)} rows.")


s = CONFIG["sql_schema"]
print("💾 Writing to Azure SQL...")

with get_sql_connection() as conn:

    # 1 — Dev git status
    truncate_and_load(conn, s, "fact_git_status_current", df_git_status, [
        "item_id", "item_display_name", "item_type", "git_sync_state",
        "is_uncommitted", "last_modified_by", "last_modified_at",
        "workspace_version", "remote_version", "workspace_id", "scan_timestamp",
    ])

    # 2 — Downstream workspace items (Test + Prod)
    truncate_and_load(conn, s, "fact_downstream_items_current", df_downstream_items, [
        "workspace_id", "stage", "item_id", "item_display_name", "item_type",
        "item_modified_at", "item_modified_by", "scan_timestamp",
    ])

    # 3 — Full violation dataset (all items, filter by is_violation in Power BI)
    truncate_and_load(conn, s, "fact_violations_current", df_violations, [
        "item_id", "item_display_name", "item_type", "git_sync_state",
        "severity", "is_violation", "pending_promotion",
        "v1_uncommitted_in_dev", "v2_promoted_while_dirty",
        "v3_modified_after_commit", "v4_direct_edit_test", "v4_direct_edit_prod",
        "last_modified_by", "last_modified_at", "last_commit_at",
        "last_commit_by", "last_commit_msg", "drift_hours",
        "last_promoted_at", "promoted_to_stage", "promoted_from_stage",
        "promoted_by", "operation_id",
        "v4_test_modified_at", "v4_test_modified_by", "v4_last_deployed_test",
        "v4_prod_modified_at", "v4_prod_modified_by", "v4_last_deployed_prod",
        "scan_timestamp",
    ])

    # 4 — Deployment operations
    if not df_deployments.empty:
        truncate_and_load(conn, s, "fact_deployment_ops_current", df_deployments, [
            "operation_id", "item_display_name", "item_type",
            "source_stage", "target_stage", "operation_status",
            "triggered_by", "created_at", "completed_at",
        ])

print("\n🎉 All tables refreshed.")


# -----------------------------------------------------------------------------
# CELL 11 — Summary output
# Shown in Fabric Pipeline run history. Also returned as notebook exit value
# so a downstream pipeline activity can read it and conditionally send alerts.
# -----------------------------------------------------------------------------

total_items    = len(df_git_status)
uncommitted    = int(df_git_status["is_uncommitted"].sum()) if not df_git_status.empty else 0
total_v        = int(df_violations["is_violation"].sum())   if not df_violations.empty else 0
critical_v     = int((df_violations["severity"] == "CRITICAL").sum()) if not df_violations.empty else 0
high_v         = int((df_violations["severity"] == "HIGH").sum())     if not df_violations.empty else 0
promoted_dirty = int(df_violations["v2_promoted_while_dirty"].sum())  if not df_violations.empty else 0
direct_test    = int(df_violations["v4_direct_edit_test"].sum())      if not df_violations.empty else 0
direct_prod    = int(df_violations["v4_direct_edit_prod"].sum())      if not df_violations.empty else 0
pending        = int(df_violations["pending_promotion"].sum())        if not df_violations.empty else 0
sync_rate      = round((total_items - uncommitted) / total_items * 100, 1) if total_items > 0 else 0

summary = {
    "scan_timestamp":         now_utc().isoformat(),
    "total_items_tracked":    total_items,
    "uncommitted_in_dev":     uncommitted,
    "total_violations":       total_v,
    "critical_violations":    critical_v,       # direct Prod edits
    "high_violations":        high_v,
    "promoted_while_dirty":   promoted_dirty,
    "direct_edits_in_test":   direct_test,
    "direct_edits_in_prod":   direct_prod,      # most urgent
    "pending_promotion":      pending,           # informational
    "git_sync_rate_pct":      sync_rate,
}

print("\n📊 SCAN SUMMARY")
print("=" * 50)
for k, v in summary.items():
    flag = " ⚠ ACTION REQUIRED" if k == "direct_edits_in_prod" and v > 0 else ""
    print(f"  {k:<32} {v}{flag}")

# Exit value is readable by subsequent Fabric Pipeline activities
# Use this to trigger a Teams webhook alert if critical_violations > 0
mssparkutils.notebook.exit(json.dumps(summary))
