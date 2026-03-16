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


def parse_dt(s) -> Optional[datetime]:
    """
    Parse a datetime value → UTC-aware datetime. Returns None if blank/invalid.
    Handles:
      - ISO 8601 strings  e.g. "2026-03-09T18:23:10.873Z"
      - Integer epoch milliseconds e.g. 1741542190873
      - Integer epoch seconds e.g. 1741542190
    """
    if not s and s != 0:
        return None
    try:
        # Integer — treat as epoch. >1e10 means milliseconds, else seconds.
        if isinstance(s, (int, float)):
            ts = s / 1000 if s > 1e10 else s
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        # String — parse ISO 8601
        return datetime.fromisoformat(
            str(s).replace("Z", "+00:00")
        ).astimezone(timezone.utc)
    except Exception:
        return None


def name_key(s: Optional[str]) -> str:
    """Normalise an item display name for fuzzy joining across workspaces."""
    if not s:
        return ""
    return s.lower().strip()

# ---------------------------------------------------------------------------
# User GUID → UPN lookup
# Built once per scan from the Dev workspace member list.
# Fabric returns the same user GUIDs across workspaces in the same tenant,
# so one workspace member list covers the whole pipeline.
# ---------------------------------------------------------------------------

_user_lookup: dict = {}   # module-level cache — populated in Cell 4 setup


def build_user_lookup() -> dict:
    """
    Calls GET /workspaces/{id}/roleAssignments to get all workspace members.
    Returns a dict of {user_id_guid: userPrincipalName}.
    
    Falls back gracefully — if a GUID isn't found, the GUID itself is returned
    so no data is lost, just less readable.
    """
    print("🔍 Building user GUID → UPN lookup from workspace members...")

    lookup = {}

    # Pull members from all three workspaces — same tenant, same user pool,
    # but team members may only appear in some workspaces
    workspace_ids = [
        CONFIG["dev_workspace_id"],
        CONFIG["test_workspace_id"],
        CONFIG["prod_workspace_id"],
    ]

    for ws_id in workspace_ids:
        try:
            data    = fabric_get(f"workspaces/{ws_id}/roleAssignments")
            members = data.get("value", [])

            for member in members:
                principal = member.get("principal", {})
                guid      = principal.get("id")
                upn       = principal.get("userPrincipalName") or \
                            principal.get("displayName")        or \
                            guid   # fallback to GUID if UPN unavailable

                if guid and guid not in lookup:
                    lookup[guid] = upn

        except Exception as e:
            print(f"   ⚠ Could not fetch members for workspace {ws_id}: {e}")

    print(f"   {len(lookup)} unique users mapped.")
    return lookup


def resolve_user(guid: Optional[str]) -> Optional[str]:
    """Resolve a user GUID to UPN using the cached lookup. Returns GUID if not found."""
    if not guid:
        return None
    return _user_lookup.get(guid, guid)


print("✅ Cell 3 — imports and helpers ready.")


# -----------------------------------------------------------------------------
# CELL 4 — Fetch Git status for all items in Dev workspace
#
# API: GET /workspaces/{devWorkspaceId}/git/status
#
# Fabric response structure (confirmed from diagnostic):
#   raw["workspaceHead"]    — commit hash string (current workspace state)
#   raw["remoteCommitHash"] — commit hash string (current remote/ADO state)
#   raw["changes"]          — list of items that differ from remote
#
# Each change object:
#   change["itemMetadata"]["itemIdentifier"]["objectId"] — item ID
#   change["itemMetadata"]["displayName"]                — item name
#   change["itemMetadata"]["itemType"]                   — e.g. SynapseNotebook
#   change["workspaceChange"]  — string or None: 'Added', 'Modified', 'Deleted'
#   change["remoteChange"]     — string or None: 'Added', 'Modified', 'Deleted'
#   change["conflictType"]     — string: 'None' or actual conflict type
#
# git_sync_state derivation:
#   workspaceChange set, remoteChange None → Uncommitted (local change not in repo)
#   remoteChange set, workspaceChange None → NotInWorkspace (in repo, not in workspace)
#   both set OR conflictType != 'None'     → Conflict
#   neither set                            → Committed (won't appear in changes list)
#
# Note: last_modified_by and last_modified_at are not returned by this API
# version on a per-item basis. These fields are set to None here and will
# be populated from the ADO commit history in Cell 7 where available.
# -----------------------------------------------------------------------------

def get_dev_git_status() -> pd.DataFrame:
    print("🔍 [Dev] Fetching Git status...")

    raw                   = fabric_get(f"workspaces/{CONFIG['dev_workspace_id']}/git/status")
    scan_time             = now_utc()
    workspace_commit_hash = raw.get("workspaceHead")    # plain commit hash string
    remote_commit_hash    = raw.get("remoteCommitHash") # plain commit hash string
    rows                  = []

    for change in raw.get("changes", []):

        meta          = change.get("itemMetadata", {}) or {}
        identifier    = meta.get("itemIdentifier", {}) or {}
        ws_change     = change.get("workspaceChange")   # 'Added', 'Modified', 'Deleted', or None
        remote_change = change.get("remoteChange")      # 'Added', 'Modified', 'Deleted', or None
        conflict_type = change.get("conflictType", "None")

        # Derive a normalised git sync state from the raw change flags
        if conflict_type and conflict_type != "None":
            git_sync_state = "Conflict"
        elif ws_change and not remote_change:
            git_sync_state = "Uncommitted"
        elif remote_change and not ws_change:
            git_sync_state = "NotInWorkspace"
        elif ws_change and remote_change:
            git_sync_state = "Conflict"
        else:
            git_sync_state = "Committed"

        raw_type       = meta.get("itemType") or ""
        item_type      = raw_type.replace("Synapse", "")

        rows.append({
            "scan_timestamp":    scan_time,
            "item_id":           identifier.get("objectId"),
            "item_display_name": meta.get("displayName"),
            "item_type":         item_type,
            "git_sync_state":    git_sync_state,
            "workspace_change":  ws_change,
            "remote_change":     remote_change,
            "conflict_type":     conflict_type,
            "workspace_version": workspace_commit_hash,
            "remote_version":    remote_commit_hash,
            "last_modified_by":  None,   # not available per-item in this API version
            "last_modified_at":  None,   # not available per-item in this API version
            "is_uncommitted":    git_sync_state == "Uncommitted",
            "workspace_id":      CONFIG["dev_workspace_id"],
            "_name_key":         name_key(meta.get("displayName")),
        })

    df = pd.DataFrame(rows)
    uncommitted = int(df["is_uncommitted"].sum()) if not df.empty else 0
    print(f"   {len(df)} items found — {uncommitted} uncommitted.")
    return df


df_git_status = get_dev_git_status()
display(df_git_status[["item_display_name", "item_type", "git_sync_state",
                        "workspace_change", "remote_change"]])

# -----------------------------------------------------------------------------
# CELL 5 — Fetch item lists from Test and Prod workspaces
#
# API: GET /workspaces/{workspaceId}/items
#
# These workspaces have no Git connection. We capture the item list so we
# can cross-reference against deployment history in Cell 8 to detect items
# that exist in Test or Prod with no corresponding deployment record —
# indicating they were created or edited directly in that workspace.
#
# Note: modifiedDateTime and modifiedBy are not returned by this API version.
# v4 detection therefore relies on deployment history as the source of truth
# rather than timestamp comparison.
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
            "workspace_id":      workspace_id,
            "stage":             stage_label,
            "item_id":           item.get("id"),
            "item_display_name": item.get("displayName"),
            "item_type":         item.get("type"),
            "item_modified_at":  None,   # not returned by this API version
            "item_modified_by":  None,   # not returned by this API version
            "scan_timestamp":    scan_time,
            "_name_key":         name_key(item.get("displayName")),
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

df_downstream_items = pd.concat(
    [df_test_items, df_prod_items], ignore_index=True
)

display(df_downstream_items[["stage", "item_display_name", "item_type",
                              "item_modified_at", "item_modified_by"]].head(20))

# -----------------------------------------------------------------------------
# CELL 6 — Fetch deployment pipeline operations (promotion history)
#
# Confirmed API response structure (from diagnostic output):
#   operation fields:
#     id, type, status, executionStartTime, executionEndTime,
#     sourceStageId, targetStageId, performedBy, executionPlan
#
#   Deployed items are NOT at the top level.
#   They live at: executionPlan.steps[].sourceAndTarget
#     sourceAndTarget.sourceItemId      — item GUID
#     sourceAndTarget.sourceItemDisplayName — item display name  ← key field
#     sourceAndTarget.itemType          — e.g. "Notebook"
#   Each step also has:
#     step.status                       — "Succeeded" / "Failed"
#     step.preDeploymentDiffState       — "New" / "Different" / "NoDifference"
# -----------------------------------------------------------------------------


# ── User GUID → UPN lookup ───────────────────────────────────────────────────
# Defined here as well as Cell 3 so Cell 6 is self-contained when run
# individually. The global _user_lookup cache prevents double-fetching.

def build_user_lookup() -> dict:
    print("   👤 Building user GUID → UPN lookup...")
    lookup = {}
    for ws_id in [CONFIG["dev_workspace_id"],
                  CONFIG["test_workspace_id"],
                  CONFIG["prod_workspace_id"]]:
        try:
            data = fabric_get(f"workspaces/{ws_id}/roleAssignments")
            for member in data.get("value", []):
                principal = member.get("principal", {})
                guid      = principal.get("id")
                upn       = (principal.get("userPrincipalName") or
                             principal.get("displayName") or guid)
                if guid and guid not in lookup:
                    lookup[guid] = upn
        except Exception as e:
            print(f"   ⚠ Could not fetch members for workspace {ws_id}: {e}")
    print(f"   {len(lookup)} unique users mapped.")
    return lookup


def resolve_user(guid: str = None) -> str:
    if not guid:
        return None
    return _user_lookup.get(guid, guid)


# ── Stage GUID → name lookup ─────────────────────────────────────────────────

def get_pipeline_stages() -> dict:
    print("   🗂 Fetching pipeline stage definitions...")
    fallback_names = ["Dev", "Test", "Prod"]
    try:
        data   = fabric_get(f"deploymentPipelines/{CONFIG['deployment_pipeline_id']}")
        stages = data.get("stages", [])
    except Exception as e:
        print(f"   ⚠ Could not fetch pipeline definition: {e}")
        return {}
    lookup = {}
    for i, stage in enumerate(stages):
        stage_id   = stage.get("id")
        stage_name = (stage.get("displayName") or
                      (fallback_names[i] if i < len(fallback_names) else f"Stage{i}"))
        lookup[stage_id] = stage_name
        print(f"      Stage {i}: {stage_name} → {stage_id}")
    return lookup


# ── Main function ─────────────────────────────────────────────────────────────

def get_deployment_operations() -> pd.DataFrame:
    """
    Fetches deployment pipeline operations and extracts deployed item detail
    from executionPlan.steps[].sourceAndTarget — confirmed structure from
    diagnostic output.

    One row per deployed item per operation. An operation that deployed
    3 items produces 3 rows, all sharing the same operation_id.
    """
    global _user_lookup
    if not _user_lookup:
        _user_lookup = build_user_lookup()

    stage_lookup = get_pipeline_stages()

    print("🔍 Fetching deployment pipeline operations...")
    cutoff = now_utc() - timedelta(days=CONFIG["lookback_days"])

    try:
        data    = fabric_get(
            f"deploymentPipelines/{CONFIG['deployment_pipeline_id']}/operations"
        )
        all_ops = data.get("value", [])
    except Exception as e:
        print(f"   ❌ Failed to fetch operations: {e}")
        return pd.DataFrame()

    print(f"   {len(all_ops)} total operations returned.")
    rows = []

    for op in all_ops:

        created_at   = parse_dt(op.get("executionStartTime"))
        completed_at = parse_dt(op.get("executionEndTime"))

        if created_at and created_at < cutoff:
            continue

        op_id        = op.get("id")
        status       = op.get("status")
        source_stage = stage_lookup.get(op.get("sourceStageId"), "Unknown")
        target_stage = stage_lookup.get(op.get("targetStageId"), "Unknown")
        performed_by = resolve_user(op.get("performedBy", {}).get("id"))

        # ── Fetch per-operation detail to get executionPlan.steps ────────────
        # The operations LIST endpoint returns metadata only — no steps.
        # The individual operation endpoint returns the full executionPlan
        # including steps[].sourceAndTarget which holds the item detail.
        # Confirmed structure from diagnostic:
        #   executionPlan.steps[].sourceAndTarget.sourceItemDisplayName  — name
        #   executionPlan.steps[].sourceAndTarget.sourceItemId       — GUID
        #   executionPlan.steps[].sourceAndTarget.itemType           — type
        #   executionPlan.steps[].status                             — Succeeded/Failed
        #   executionPlan.steps[].preDeploymentDiffState             — New/Different/etc

        steps = []
        try:
            op_detail = fabric_get(
                f"deploymentPipelines/{CONFIG['deployment_pipeline_id']}"
                f"/operations/{op_id}"
            )
            steps = op_detail.get("executionPlan", {}).get("steps", [])
            if not steps:
                print(f"   ⚠ Operation {op_id}: executionPlan.steps is empty. "
                      f"Top-level keys: {list(op_detail.keys())}")
        except Exception as e:
            print(f"   ⚠ Could not fetch detail for operation {op_id}: {e}")

        if not steps:
            rows.append({
                "operation_id":         op_id,
                "operation_type":       op.get("type"),
                "operation_status":     status,
                "source_stage":         source_stage,
                "target_stage":         target_stage,
                "created_at":           created_at,
                "completed_at":         completed_at,
                "triggered_by":         performed_by,
                "item_id":              None,
                "item_display_name":    None,
                "item_type":            None,
                "item_deploy_status":   None,
                "pre_deployment_state": None,
                "_name_key":            None,
            })
            continue

        for step in steps:
            sat       = step.get("sourceAndTarget", {}) or {}
            item_name = sat.get("sourceItemDisplayName")
            item_id   = sat.get("sourceItemId")
            # Normalise item type — Git API returns "SynapseNotebook",
            # Items API returns "Notebook". Strip "Synapse" prefix so
            # _name_key joins work correctly across both sources.
            raw_type  = sat.get("itemType") or ""
            item_type = raw_type.replace("Synapse", "")

            rows.append({
                "operation_id":         op_id,
                "operation_type":       op.get("type"),
                "operation_status":     status,
                "source_stage":         source_stage,
                "target_stage":         target_stage,
                "created_at":           created_at,
                "completed_at":         completed_at,
                "triggered_by":         performed_by,
                "item_id":              item_id,
                "item_display_name":    item_name,
                "item_type":            item_type,
                "item_deploy_status":   step.get("status"),
                "pre_deployment_state": step.get("preDeploymentDiffState"),
                "_name_key":            name_key(item_name),
            })

    df = pd.DataFrame(rows) if rows else pd.DataFrame()

    if not df.empty:
        named = df["item_display_name"].notna().sum()
        print(f"   ✅ {df['operation_id'].nunique()} operations, "
              f"{named} item rows with names, "
              f"{len(df) - named} without.")
    else:
        print("   ⚠ No deployment operation rows produced.")

    return df


# ── Run ───────────────────────────────────────────────────────────────────────

df_deployments = get_deployment_operations()
print("Shape:", df_deployments.shape)
print("Columns:", list(df_deployments.columns))
print("completed_at dtype:", df_deployments["completed_at"].dtype)
print("completed_at sample:", df_deployments["completed_at"].iloc[0])
df_deployments["created_at"]  = pd.to_datetime(df_deployments["created_at"], utc=True, errors="coerce")
df_deployments["completed_at"] = pd.to_datetime(df_deployments["completed_at"], utc=True, errors="coerce")

if not df_deployments.empty:
    display_df = df_deployments[[
        "item_display_name", "item_type", "source_stage",
        "target_stage", "triggered_by", "created_at",
        "operation_status", "item_deploy_status", "pre_deployment_state"
    ]].copy()
    display_df["created_at"]  = display_df["created_at"].astype(str).str.replace(r"\.\d+\+.*", " UTC", regex=True)
    display_df["completed_at"] = display_df["completed_at"].astype(str).str.replace(r"\.\d+\+.*", " UTC", regex=True)
    display(display_df.head(20))
else:
    print("No deployment data returned — check warnings above.")

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
    # Session guard — re-import if kernel restarted without re-running Cell 3.
    # This makes Cell 7 safe to run independently during debugging.
    import pandas as pd
    import requests
    from datetime import datetime, timezone, timedelta
    from base64   import b64encode
    from typing   import Optional

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
# Rules:
#   v1 — git_sync_state == "Uncommitted" in Dev
#   v2 — Item appears in deployment history AND is currently uncommitted
#   v3 — Fabric last_modified_at > last ADO commit_at
#        (currently NULL from API — skipped until alt source found)
#   v4 — Item exists in Test or Prod with NO deployment record in lookback
#        window → unverified origin, likely created or edited directly
#        MEDIUM if Test, CRITICAL if Prod
#
#   pending_promotion — git_sync_state == "Committed" AND last_commit_at >
#                       last_promoted_at (informational, not a violation)
#
# Severity:
#   CRITICAL — v4 direct edit in Prod
#   HIGH     — v2 promoted while dirty / multiple flags
#   MEDIUM   — v4 direct edit in Test / single promotion flag
#   LOW      — v1 uncommitted in Dev only, not yet promoted
#   NONE     — clean
# -----------------------------------------------------------------------------

def detect_violations(df_git:        pd.DataFrame,
                      df_downstream: pd.DataFrame,
                      df_deps:       pd.DataFrame,
                      df_ado:        pd.DataFrame) -> pd.DataFrame:

    print("🔎 Running violation detection...")
    scan_time = now_utc()

    # ── 1. Build last-promotion lookup ───────────────────────────────────────

    if not df_deps.empty:
        successful_deps = df_deps[df_deps["operation_status"] == "Succeeded"].copy()

        # Last promotion per item per target stage — anchors v4 detection
        last_promo_by_stage = (
            successful_deps
            .sort_values("completed_at", ascending=False)
            .groupby(["_name_key", "target_stage"])
            .first()
            .reset_index()
            [["_name_key", "target_stage", "completed_at",
              "triggered_by", "source_stage", "operation_id"]]
        )

        # Last promotion per item regardless of stage — anchors v2 detection
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
        last_promo_by_stage = pd.DataFrame(columns=[
            "_name_key", "target_stage", "completed_at",
            "triggered_by", "source_stage", "operation_id"])
        last_promo_overall = pd.DataFrame(columns=[
            "_name_key", "last_promoted_at", "promoted_to_stage",
            "promoted_by", "promoted_from_stage", "operation_id"])

    # ── 2. v4 — Detect items in Test/Prod with no deployment record ──────────
    # Since modifiedDateTime is not returned by the Fabric Items API,
    # we use deployment history as the source of truth.
    # An item that exists in Test or Prod but has no successful deployment
    # record within the lookback window has an unverified origin.

    v4_rows = []

    if not df_downstream.empty:
        for _, row in df_downstream.iterrows():
            stage  = row["stage"]
            name_k = row["_name_key"]

            # Check if a deployment record exists for this item to this stage
            has_deployment = False
            if not last_promo_by_stage.empty:
                match = (
                    (last_promo_by_stage["_name_key"]    == name_k) &
                    (last_promo_by_stage["target_stage"] == stage)
                )
                has_deployment = match.any()

            if not has_deployment:
                severity = "CRITICAL" if stage == "Prod" else "MEDIUM"
                v4_rows.append({
                    "item_display_name":      row["item_display_name"],
                    "item_type":              row["item_type"],
                    "affected_stage":         stage,
                    "item_modified_at":       None,
                    "item_modified_by":       None,
                    "last_deployed_to_stage": None,
                    "v4_direct_edit":         True,
                    "v4_severity":            severity,
                    "v4_note":                (f"No deployment record in last "
                                               f"{CONFIG['lookback_days']} days — "
                                               f"unverified origin in {stage}"),
                    "_name_key":              name_k,
                })

    df_v4 = pd.DataFrame(v4_rows) if v4_rows else pd.DataFrame(
        columns=["item_display_name", "item_type", "affected_stage",
                 "item_modified_at", "item_modified_by",
                 "last_deployed_to_stage", "v4_direct_edit",
                 "v4_severity", "v4_note", "_name_key"])

    critical_count = int((df_v4["v4_severity"] == "CRITICAL").sum()) if not df_v4.empty else 0
    print(f"   v4 unverified items: {len(df_v4)} "
          f"({critical_count} CRITICAL in Prod)")

    # ── 3. Build main violation DataFrame from Dev git status ────────────────

    df = df_git.copy()

    # Join last overall promotion onto Dev rows
    df = df.merge(last_promo_overall, on="_name_key", how="left")

    # Join ADO commit history
    if not df_ado.empty:
        ado_lookup = df_ado.set_index("_name_key")[
            ["last_commit_at", "last_commit_by",
             "last_commit_msg", "last_commit_id"]
        ]
        df = df.merge(ado_lookup, left_on="_name_key",
                      right_index=True, how="left")
    else:
        df["last_commit_at"]  = pd.NaT
        df["last_commit_by"]  = None
        df["last_commit_msg"] = None
        df["last_commit_id"]  = None

    # ── 4. v1, v2, v3 flags ─────────────────────────────────────────────────

    df["v1_uncommitted_in_dev"] = df["is_uncommitted"]

    df["v2_promoted_while_dirty"] = (
        df["last_promoted_at"].notna() & df["is_uncommitted"]
    )

    # v3 requires last_modified_at from Dev — currently NULL from API.
    # Defaulting to False until an alternative data source is identified.
    df["v3_modified_after_commit"] = False

    # ── 5. pending_promotion (informational — not a violation) ───────────────

    df["pending_promotion"] = False
    pp_mask = (
        (df["git_sync_state"] == "Committed") &
        df["last_commit_at"].notna() &
        (
            df["last_promoted_at"].isna() |
            (df["last_commit_at"] > df["last_promoted_at"])
        )
    )
    df.loc[pp_mask, "pending_promotion"] = True

    # ── 6. Join v4 flags back onto Dev rows ──────────────────────────────────
    # v4 violations originate in downstream workspaces. We join them back
    # onto Dev rows by name so Power BI has one unified fact table.
    # Items in Test/Prod that have no matching Dev row are appended below.

    df["v4_direct_edit_test"]   = False
    df["v4_direct_edit_prod"]   = False
    df["v4_test_modified_at"]   = pd.NaT
    df["v4_prod_modified_at"]   = pd.NaT
    df["v4_test_modified_by"]   = None
    df["v4_prod_modified_by"]   = None
    df["v4_last_deployed_test"] = pd.NaT
    df["v4_last_deployed_prod"] = pd.NaT
    df["v4_note"]               = None

    if not df_v4.empty:
        for _, v4row in df_v4.iterrows():
            key   = v4row["_name_key"]
            stage = v4row["affected_stage"]
            match = df["_name_key"] == key

            if match.any():
                if stage == "Test":
                    df.loc[match, "v4_direct_edit_test"]   = True
                    df.loc[match, "v4_test_modified_at"]   = v4row["item_modified_at"]
                    df.loc[match, "v4_test_modified_by"]   = v4row["item_modified_by"]
                    df.loc[match, "v4_last_deployed_test"] = v4row["last_deployed_to_stage"]
                    df.loc[match, "v4_note"]               = v4row["v4_note"]
                elif stage == "Prod":
                    df.loc[match, "v4_direct_edit_prod"]   = True
                    df.loc[match, "v4_prod_modified_at"]   = v4row["item_modified_at"]
                    df.loc[match, "v4_prod_modified_by"]   = v4row["item_modified_by"]
                    df.loc[match, "v4_last_deployed_prod"] = v4row["last_deployed_to_stage"]
                    df.loc[match, "v4_note"]               = v4row["v4_note"]
            else:
                # Item exists in Test/Prod but not in Dev git status at all
                # Append as a standalone violation row
                new_row = {col: None for col in df.columns}
                new_row.update({
                    "scan_timestamp":        scan_time,
                    "item_display_name":     v4row["item_display_name"],
                    "item_type":             v4row["item_type"],
                    "git_sync_state":        "Unknown",
                    "is_uncommitted":        False,
                    "workspace_id":          CONFIG["dev_workspace_id"],
                    "_name_key":             key,
                    "v1_uncommitted_in_dev": False,
                    "v2_promoted_while_dirty":  False,
                    "v3_modified_after_commit": False,
                    "pending_promotion":     False,
                    "v4_direct_edit_test":   stage == "Test",
                    "v4_direct_edit_prod":   stage == "Prod",
                    "v4_note":               v4row["v4_note"],
                })
                df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

    # ── 7. Composite violation flag ──────────────────────────────────────────

    df["is_violation"] = (
        df["v1_uncommitted_in_dev"]    |
        df["v2_promoted_while_dirty"]  |
        df["v3_modified_after_commit"] |
        df["v4_direct_edit_test"]      |
        df["v4_direct_edit_prod"]
    )

    # ── 8. Severity ──────────────────────────────────────────────────────────

    def severity(row) -> str:
        if row["v4_direct_edit_prod"]:
            return "CRITICAL"
        score = (
            int(bool(row["v1_uncommitted_in_dev"]))    * 1 +
            int(bool(row["v2_promoted_while_dirty"]))  * 2 +
            int(bool(row["v3_modified_after_commit"])) * 1 +
            int(bool(row["v4_direct_edit_test"]))      * 2
        )
        if   score >= 4: return "HIGH"
        elif score >= 2: return "MEDIUM"
        elif score >= 1: return "LOW"
        return "NONE"

    df["severity"] = df.apply(severity, axis=1)

    # ── 9. Drift hours ───────────────────────────────────────────────────────
    # Requires last_modified_at — currently NULL. Placeholder for future use.
    df["drift_hours"] = None

    df["scan_timestamp"] = scan_time

    # ── 10. Summary ──────────────────────────────────────────────────────────
    total_v    = int(df["is_violation"].sum())
    critical_v = int((df["severity"] == "CRITICAL").sum())
    high_v     = int((df["severity"] == "HIGH").sum())
    pending_v  = int(df["pending_promotion"].sum())
    v4_prod_v  = int(df["v4_direct_edit_prod"].sum())

    print(f"   ✅ Detection complete.")
    print(f"      Violations    : {total_v} (CRITICAL={critical_v}, HIGH={high_v})")
    print(f"      Direct Prod   : {v4_prod_v}")
    print(f"      Pending promo : {pending_v} (informational)")

    return df


# CORRECT:
df_git_status["last_modified_at"] = pd.to_datetime(df_git_status["last_modified_at"], utc=True, errors="coerce")

df_violations = detect_violations(
    df_git_status,
    df_downstream_items,
    df_deployments,
    df_ado_commits
)

# Preview
cols_preview = [
    "item_display_name", "item_type", "severity",
    "v1_uncommitted_in_dev", "v2_promoted_while_dirty",
    "v4_direct_edit_test", "v4_direct_edit_prod",
    "pending_promotion", "v4_note"
]
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
        operation_id          NVARCHAR(128),
        item_display_name     NVARCHAR(256),
        item_type             NVARCHAR(64),
        source_stage          NVARCHAR(32),
        target_stage          NVARCHAR(32),
        operation_status      NVARCHAR(32),
        triggered_by          NVARCHAR(256),
        created_at            DATETIME2,
        completed_at          DATETIME2,
        item_deploy_status    NVARCHAR(32),    -- step-level status per item
        pre_deployment_state  NVARCHAR(64)     -- New / Different / NoDifference
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
            "item_deploy_status", "pre_deployment_state",
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
