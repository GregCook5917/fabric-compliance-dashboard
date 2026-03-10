# =============================================================================
# FABRIC GIT GOVERNANCE SCANNER
# Fabric Notebook — run via Fabric Pipeline on a schedule
#
# What this does:
#   1. Calls Fabric REST API to get Git status of all items in Dev workspace
#   2. Calls Fabric REST API to get recent deployment pipeline operations
#   3. Cross-references to find items promoted while uncommitted
#   4. Overwrites Azure SQL tables with current state
#
# Authentication: PAT-based (Fabric + ADO)
# Target:         Azure SQL Database
# Mode:           Overwrite (latest state only)
# =============================================================================


# -----------------------------------------------------------------------------
# CELL 1 — Install dependencies
# (Run once; comment out after first execution if scheduling via Pipeline)
# -----------------------------------------------------------------------------

# %pip install pyodbc azure-identity requests pandas --quiet


# -----------------------------------------------------------------------------
# CELL 2 — Configuration
# Edit these values before running. Store secrets in Fabric Environment or
# Azure Key Vault — do NOT hardcode PATs in production.
# -----------------------------------------------------------------------------

CONFIG = {

    # --- Fabric ---
    # Your Fabric PAT: Fabric portal → top-right profile → Developer settings
    "fabric_pat": "YOUR_FABRIC_PAT_HERE",

    # The workspace ID of your DEV workspace (Git-connected)
    # Found in the URL: app.powerbi.com/groups/{WORKSPACE_ID}/...
    "dev_workspace_id": "YOUR_DEV_WORKSPACE_ID",

    # Your Fabric Deployment Pipeline ID
    # Found via: GET https://api.fabric.microsoft.com/v1/deploymentPipelines
    # Or in the URL when viewing the pipeline in Fabric
    "deployment_pipeline_id": "YOUR_DEPLOYMENT_PIPELINE_ID",

    # How many days of deployment operations to look back
    "lookback_days": 30,

    # --- Azure DevOps ---
    # ADO PAT: dev.azure.com → User Settings → Personal Access Tokens
    # Required scopes: Code (Read), Project and Team (Read)
    "ado_pat": "YOUR_ADO_PAT_HERE",
    "ado_organization": "YOUR_ORG_NAME",       # e.g. "contoso"
    "ado_project":      "YOUR_PROJECT_NAME",    # e.g. "FabricBI"
    "ado_repository":   "YOUR_REPO_NAME",       # e.g. "fabric-workspace"

    # --- Azure SQL ---
    # Use ODBC Driver 18. Format: server.database.windows.net
    "sql_server":   "YOUR_SERVER.database.windows.net",
    "sql_database": "YOUR_DATABASE_NAME",
    "sql_username": "YOUR_SQL_USERNAME",
    "sql_password": "YOUR_SQL_PASSWORD",
    "sql_schema":   "governance",               # Schema for all tables
}

# Fabric API base URL
FABRIC_API = "https://api.fabric.microsoft.com/v1"
ADO_API    = "https://dev.azure.com"


# -----------------------------------------------------------------------------
# CELL 3 — Imports and helpers
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
    """Auth headers for Fabric REST API using PAT."""
    token = b64encode(f":{CONFIG['fabric_pat']}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Content-Type":  "application/json",
    }


def ado_headers() -> dict:
    """Auth headers for Azure DevOps REST API using PAT."""
    token = b64encode(f":{CONFIG['ado_pat']}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Content-Type":  "application/json",
    }


def fabric_get(path: str, params: dict = None) -> dict:
    """GET from Fabric API with basic error handling."""
    url = f"{FABRIC_API}/{path}"
    resp = requests.get(url, headers=fabric_headers(), params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def ado_get(path: str, params: dict = None) -> dict:
    """GET from ADO API with basic error handling."""
    url = f"{ADO_API}/{path}"
    resp = requests.get(url, headers=ado_headers(), params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(s: Optional[str]) -> Optional[datetime]:
    """Parse ISO datetime string to UTC datetime, return None if missing."""
    if not s:
        return None
    try:
        # Handle both Z suffix and +00:00
        s = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None


print("✅ Imports and helpers loaded.")


# -----------------------------------------------------------------------------
# CELL 4 — Fetch Git status for all items in Dev workspace
# Returns a list of dicts with item metadata + git sync state
# -----------------------------------------------------------------------------

def get_dev_workspace_git_status() -> pd.DataFrame:
    """
    Calls GET /workspaces/{id}/git/status
    Returns each workspace item with its current git sync state.

    gitSyncState values:
        Committed       — item matches what's in the repo ✅
        Uncommitted     — item has local changes not yet pushed ❌
        Conflict        — diverged from remote ⚠
        NotInWorkspace  — in repo but not in workspace
        NotInGit        — in workspace but not tracked by git
    """
    print("🔍 Fetching Git status from Dev workspace...")

    data = fabric_get(f"workspaces/{CONFIG['dev_workspace_id']}/git/status")

    rows = []
    scan_time = now_utc()

    for change in data.get("changes", []):
        ws_item    = change.get("item", {})
        ws_head    = change.get("workspaceHead", {})
        remote_head = change.get("remoteCommittedHead", {})

        rows.append({
            "scan_timestamp":       scan_time,
            "item_id":              ws_item.get("objectId"),
            "item_display_name":    ws_item.get("displayName"),
            "item_type":            ws_item.get("objectType"),
            "git_sync_state":       ws_head.get("state"),        # Key field
            "workspace_version":    ws_head.get("version"),
            "remote_version":       remote_head.get("version"),
            "last_modified_by":     ws_head.get("lastModifiedBy", {}).get("userPrincipalName"),
            "last_modified_at":     parse_dt(ws_head.get("lastModifiedAt")),
            "is_uncommitted":       ws_head.get("state") == "Uncommitted",
            "workspace_id":         CONFIG["dev_workspace_id"],
        })

    df = pd.DataFrame(rows)
    print(f"   Found {len(df)} tracked items. "
          f"{df['is_uncommitted'].sum()} uncommitted.")
    return df


df_git_status = get_dev_workspace_git_status()
display(df_git_status[["item_display_name", "item_type", "git_sync_state",
                         "last_modified_by", "last_modified_at"]].head(20))


# -----------------------------------------------------------------------------
# CELL 5 — Fetch deployment pipeline operations (promotion history)
# -----------------------------------------------------------------------------

def get_deployment_operations() -> pd.DataFrame:
    """
    Calls GET /deploymentPipelines/{id}/operations
    Returns a list of recent deployments with timestamps and stage info.
    Filtered to lookback window defined in CONFIG.
    """
    print("🔍 Fetching deployment pipeline operations...")

    cutoff = now_utc() - timedelta(days=CONFIG["lookback_days"])

    # Fabric paginates — handle continuationToken
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
            continue  # Outside lookback window

        # Each operation has a list of deployed items
        for item in op.get("deployedArtifacts", []):
            rows.append({
                "operation_id":          op.get("id"),
                "operation_type":        op.get("type"),         # Deploy
                "operation_status":      op.get("status"),       # Succeeded / Failed
                "source_stage_order":    op.get("sourceStageOrder"),  # 0=Dev,1=Test,2=Prod
                "target_stage_order":    op.get("targetStageOrder"),
                "created_at":            created_at,
                "completed_at":          parse_dt(op.get("completedTime")),
                "triggered_by":          op.get("executionPlan", {})
                                           .get("triggeredBy", {})
                                           .get("userPrincipalName"),
                "item_id":               item.get("sourceObjectId"),
                "item_display_name":     item.get("displayName"),
                "item_type":             item.get("objectType"),
            })

    df = pd.DataFrame(rows) if rows else pd.DataFrame()

    # Map stage order to friendly names
    stage_map = {0: "Dev", 1: "Test", 2: "Prod"}
    if not df.empty:
        df["source_stage"] = df["source_stage_order"].map(stage_map)
        df["target_stage"] = df["target_stage_order"].map(stage_map)

    print(f"   Found {len(df)} deployment item records in last "
          f"{CONFIG['lookback_days']} days.")
    return df


df_deployments = get_deployment_operations()
if not df_deployments.empty:
    display(df_deployments[["item_display_name", "item_type", "source_stage",
                              "target_stage", "triggered_by", "created_at"]].head(20))


# -----------------------------------------------------------------------------
# CELL 6 — Fetch ADO commit history (last commit per file path)
# -----------------------------------------------------------------------------

def get_ado_last_commits() -> pd.DataFrame:
    """
    Calls ADO Git API to get the most recent commit per item path.
    Fabric stores items as folders in the repo (e.g. /SalesReport.Report/).
    We fetch recent commits and build a map of path → last commit time + author.
    """
    print("🔍 Fetching ADO commit history...")

    org     = CONFIG["ado_organization"]
    project = CONFIG["ado_project"]
    repo    = CONFIG["ado_repository"]
    cutoff  = (now_utc() - timedelta(days=CONFIG["lookback_days"])).strftime("%Y-%m-%dT%H:%M:%SZ")

    path     = f"{org}/{project}/_apis/git/repositories/{repo}/commits"
    params   = {
        "searchCriteria.fromDate": cutoff,
        "$top":                    500,
        "api-version":             "7.1",
    }

    data    = ado_get(path, params=params)
    commits = data.get("value", [])

    # For each commit, fetch the changed items to map path → commit
    path_commit_map = {}  # path → {commit_id, author, date}

    for commit in commits:
        commit_id  = commit.get("commitId")
        author     = commit.get("author", {}).get("email")
        commit_dt  = parse_dt(commit.get("author", {}).get("date"))
        comment    = commit.get("comment", "")

        # Fetch changes for this commit
        changes_path   = (f"{org}/{project}/_apis/git/repositories/"
                          f"{repo}/commits/{commit_id}/changes")
        changes_params = {"api-version": "7.1"}

        try:
            changes_data = ado_get(changes_path, params=changes_params)
            for change in changes_data.get("changes", []):
                item_path = change.get("item", {}).get("path", "")
                # Fabric items are top-level folders — grab root folder name
                parts = [p for p in item_path.strip("/").split("/") if p]
                if not parts:
                    continue
                root_path = "/" + parts[0]

                # Keep only the most recent commit per path
                if (root_path not in path_commit_map or
                        commit_dt > path_commit_map[root_path]["commit_date"]):
                    path_commit_map[root_path] = {
                        "repo_path":       root_path,
                        "last_commit_id":  commit_id,
                        "last_commit_by":  author,
                        "last_commit_at":  commit_dt,
                        "last_commit_msg": comment[:200],
                    }
        except Exception as e:
            print(f"   ⚠ Could not fetch changes for commit {commit_id}: {e}")
            continue

    df = pd.DataFrame(list(path_commit_map.values()))
    print(f"   Mapped {len(df)} unique item paths to their last commit.")
    return df


df_ado_commits = get_ado_last_commits()
if not df_ado_commits.empty:
    display(df_ado_commits.head(10))


# -----------------------------------------------------------------------------
# CELL 7 — Detect violations
# An item is a VIOLATION if it was promoted while its git state was Uncommitted.
# We approximate this by: item appears in deployment ops AND is currently
# Uncommitted AND its last_modified_at is AFTER its last ADO commit.
# -----------------------------------------------------------------------------

def detect_violations(df_git:   pd.DataFrame,
                       df_deps:  pd.DataFrame,
                       df_ado:   pd.DataFrame) -> pd.DataFrame:
    """
    Cross-reference git status, deployments, and ADO commits to flag violations.

    Violation criteria (any one is sufficient):
      V1 — Item is currently Uncommitted in Dev (drift exists now)
      V2 — Item was promoted (exists in df_deps) AND is Uncommitted (promoted dirty)
      V3 — Item's last_modified_at in Fabric > last_commit_at in ADO (change without commit)
    """
    print("🔎 Detecting violations...")

    scan_time = now_utc()

    # Normalise display names for join (lowercase, strip whitespace)
    df_git  = df_git.copy()
    df_deps = df_deps.copy() if not df_deps.empty else pd.DataFrame()
    df_ado  = df_ado.copy()  if not df_ado.empty  else pd.DataFrame()

    df_git["_name_key"] = df_git["item_display_name"].str.lower().str.strip()

    # Get most recent promotion per item
    if not df_deps.empty:
        df_deps["_name_key"] = df_deps["item_display_name"].str.lower().str.strip()
        last_promotion = (
            df_deps.sort_values("created_at", ascending=False)
                   .groupby("_name_key")
                   .first()
                   .reset_index()
            [["_name_key", "created_at", "target_stage", "triggered_by",
              "source_stage", "operation_id"]]
        )
        last_promotion.columns = ["_name_key", "last_promoted_at",
                                   "promoted_to_stage", "promoted_by",
                                   "promoted_from_stage", "operation_id"]
    else:
        last_promotion = pd.DataFrame(
            columns=["_name_key", "last_promoted_at", "promoted_to_stage",
                     "promoted_by", "promoted_from_stage", "operation_id"])

    # Build ADO path lookup: strip leading slash, strip .Report/.Dataset etc
    # Fabric repo paths are like /Sales Dashboard.Report  →  key: "sales dashboard"
    if not df_ado.empty:
        df_ado["_name_key"] = (df_ado["repo_path"]
                                .str.lstrip("/")
                                .str.split(".")
                                .str[0]
                                .str.lower()
                                .str.strip())
        ado_lookup = df_ado.set_index("_name_key")[
            ["last_commit_at", "last_commit_by", "last_commit_msg", "last_commit_id"]
        ]
    else:
        ado_lookup = pd.DataFrame()

    # Merge everything onto git status
    df = df_git.merge(last_promotion, on="_name_key", how="left")

    if not ado_lookup.empty:
        df = df.merge(ado_lookup, left_on="_name_key", right_index=True, how="left")
    else:
        df["last_commit_at"]  = None
        df["last_commit_by"]  = None
        df["last_commit_msg"] = None
        df["last_commit_id"]  = None

    # --- Violation flags ---

    # V1: Currently uncommitted in Dev
    df["v1_uncommitted_in_dev"] = df["is_uncommitted"]

    # V2: Was promoted AND is currently uncommitted
    df["v2_promoted_while_dirty"] = (
        df["last_promoted_at"].notna() & df["is_uncommitted"]
    )

    # V3: Fabric modified_at is newer than last ADO commit (unsaved change)
    df["v3_modified_after_commit"] = False
    mask = df["last_modified_at"].notna() & df["last_commit_at"].notna()
    df.loc[mask, "v3_modified_after_commit"] = (
        df.loc[mask, "last_modified_at"] > df.loc[mask, "last_commit_at"]
    )

    # Composite: any violation flag
    df["is_violation"] = (
        df["v1_uncommitted_in_dev"] |
        df["v2_promoted_while_dirty"] |
        df["v3_modified_after_commit"]
    )

    # Severity scoring
    def severity(row):
        score = (row["v1_uncommitted_in_dev"] * 1 +
                 row["v2_promoted_while_dirty"] * 2 +
                 row["v3_modified_after_commit"] * 1)
        if score >= 3:   return "HIGH"
        elif score == 2: return "MEDIUM"
        elif score == 1: return "LOW"
        return "NONE"

    df["severity"] = df.apply(severity, axis=1)

    # Drift duration in hours
    df["drift_hours"] = None
    mask2 = df["last_modified_at"].notna() & df["last_commit_at"].notna()
    df.loc[mask2, "drift_hours"] = (
        (df.loc[mask2, "last_modified_at"] - df.loc[mask2, "last_commit_at"])
        .dt.total_seconds() / 3600
    ).round(1)

    df["scan_timestamp"] = scan_time

    violations = df[df["is_violation"]].copy()
    print(f"   ✅ Scan complete. {len(violations)} violations found "
          f"({df['v2_promoted_while_dirty'].sum()} promoted-while-dirty).")
    return df  # Return full dataset; SQL write filters as needed


df_violations = detect_violations(df_git_status, df_deployments, df_ado_commits)
display(df_violations[df_violations["is_violation"]][
    ["item_display_name", "item_type", "git_sync_state", "severity",
     "v2_promoted_while_dirty", "promoted_to_stage", "promoted_by",
     "drift_hours"]
])


# -----------------------------------------------------------------------------
# CELL 8 — SQL connection helper
# Uses pyodbc with Azure SQL. Uses ODBC Driver 18 (pre-installed on Fabric).
# -----------------------------------------------------------------------------

def get_sql_connection():
    """Return a pyodbc connection to Azure SQL."""
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={CONFIG['sql_server']};"
        f"DATABASE={CONFIG['sql_database']};"
        f"UID={CONFIG['sql_username']};"
        f"PWD={CONFIG['sql_password']};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
        f"Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)


def ensure_schema_and_tables(conn):
    """
    Create schema and tables if they don't exist.
    Safe to run on every execution — uses IF NOT EXISTS.
    """
    cursor = conn.cursor()
    schema = CONFIG["sql_schema"]

    ddl = f"""
    -- Schema
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema}')
        EXEC('CREATE SCHEMA {schema}');

    -- -------------------------------------------------------------------------
    -- dim_workspace_items
    -- One row per tracked item. Overwritten each scan.
    -- -------------------------------------------------------------------------
    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = 'dim_workspace_items'
    )
    CREATE TABLE {schema}.dim_workspace_items (
        item_id             NVARCHAR(128)   PRIMARY KEY,
        item_display_name   NVARCHAR(256)   NOT NULL,
        item_type           NVARCHAR(64),
        workspace_id        NVARCHAR(128),
        last_seen_scan      DATETIME2
    );

    -- -------------------------------------------------------------------------
    -- fact_git_status_current
    -- Current Git sync state per item. Truncated and reloaded each scan.
    -- -------------------------------------------------------------------------
    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = 'fact_git_status_current'
    )
    CREATE TABLE {schema}.fact_git_status_current (
        item_id             NVARCHAR(128)   NOT NULL,
        item_display_name   NVARCHAR(256),
        item_type           NVARCHAR(64),
        git_sync_state      NVARCHAR(64),
        is_uncommitted      BIT,
        last_modified_by    NVARCHAR(256),
        last_modified_at    DATETIME2,
        workspace_version   NVARCHAR(128),
        remote_version      NVARCHAR(128),
        scan_timestamp      DATETIME2,
        workspace_id        NVARCHAR(128)
    );

    -- -------------------------------------------------------------------------
    -- fact_violations_current
    -- Current violations. Truncated and reloaded each scan.
    -- -------------------------------------------------------------------------
    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = 'fact_violations_current'
    )
    CREATE TABLE {schema}.fact_violations_current (
        item_id                  NVARCHAR(128),
        item_display_name        NVARCHAR(256),
        item_type                NVARCHAR(64),
        git_sync_state           NVARCHAR(64),
        severity                 NVARCHAR(16),
        is_violation             BIT,
        v1_uncommitted_in_dev    BIT,
        v2_promoted_while_dirty  BIT,
        v3_modified_after_commit BIT,
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
        scan_timestamp           DATETIME2
    );

    -- -------------------------------------------------------------------------
    -- fact_deployment_operations_current
    -- Recent promotions within lookback window. Truncated and reloaded.
    -- -------------------------------------------------------------------------
    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = 'fact_deployment_ops_current'
    )
    CREATE TABLE {schema}.fact_deployment_ops_current (
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
    print("✅ SQL connection successful.")


# -----------------------------------------------------------------------------
# CELL 9 — Write to Azure SQL (TRUNCATE + INSERT pattern)
# -----------------------------------------------------------------------------

def truncate_and_load(conn, schema: str, table: str, df: pd.DataFrame,
                      columns: list):
    """
    Truncates the target table then bulk-inserts the DataFrame.
    Only writes columns listed in `columns` — maps to SQL column order.
    """
    if df.empty:
        print(f"   ⚠ No data for {table} — skipping.")
        return

    cursor = conn.cursor()
    cursor.execute(f"TRUNCATE TABLE {schema}.{table}")

    # Build parameterised INSERT
    placeholders = ", ".join(["?" for _ in columns])
    col_list     = ", ".join(columns)
    sql          = f"INSERT INTO {schema}.{table} ({col_list}) VALUES ({placeholders})"

    df_subset = df[columns].copy()

    # Convert NaT / NaN to None for pyodbc
    df_subset = df_subset.where(pd.notnull(df_subset), other=None)

    rows = [tuple(row) for row in df_subset.itertuples(index=False, name=None)]
    cursor.executemany(sql, rows)
    conn.commit()
    print(f"   ✅ {table}: {len(rows)} rows loaded.")


schema = CONFIG["sql_schema"]

print("💾 Writing to Azure SQL...")

with get_sql_connection() as conn:

    # 1. Git status (all tracked items)
    truncate_and_load(conn, schema, "fact_git_status_current", df_git_status, [
        "item_id", "item_display_name", "item_type", "git_sync_state",
        "is_uncommitted", "last_modified_by", "last_modified_at",
        "workspace_version", "remote_version", "scan_timestamp", "workspace_id",
    ])

    # 2. Violations (all items, is_violation flag drives Power BI filters)
    truncate_and_load(conn, schema, "fact_violations_current", df_violations, [
        "item_id", "item_display_name", "item_type", "git_sync_state",
        "severity", "is_violation", "v1_uncommitted_in_dev",
        "v2_promoted_while_dirty", "v3_modified_after_commit",
        "last_modified_by", "last_modified_at", "last_commit_at",
        "last_commit_by", "last_commit_msg", "drift_hours",
        "last_promoted_at", "promoted_to_stage", "promoted_from_stage",
        "promoted_by", "operation_id", "scan_timestamp",
    ])

    # 3. Deployment operations
    if not df_deployments.empty:
        truncate_and_load(conn, schema, "fact_deployment_ops_current",
                          df_deployments, [
            "operation_id", "item_display_name", "item_type",
            "source_stage", "target_stage", "operation_status",
            "triggered_by", "created_at", "completed_at",
        ])

print("\n🎉 Scan complete. All tables refreshed.")
print(f"   Scan timestamp: {now_utc().isoformat()}")


# -----------------------------------------------------------------------------
# CELL 10 — Summary output (visible in Fabric Pipeline run history)
# -----------------------------------------------------------------------------

total_items      = len(df_git_status)
uncommitted      = int(df_git_status["is_uncommitted"].sum())
violations       = int(df_violations["is_violation"].sum())
promoted_dirty   = int(df_violations["v2_promoted_while_dirty"].sum())
high_sev         = int((df_violations["severity"] == "HIGH").sum())

summary = {
    "scan_timestamp":       now_utc().isoformat(),
    "total_items_tracked":  total_items,
    "uncommitted_items":    uncommitted,
    "total_violations":     violations,
    "promoted_while_dirty": promoted_dirty,
    "high_severity":        high_sev,
    "git_sync_rate_pct":    round((total_items - uncommitted) / total_items * 100, 1)
                            if total_items > 0 else 0,
}

print("\n📊 SCAN SUMMARY")
print("=" * 45)
for k, v in summary.items():
    print(f"  {k:<30} {v}")

# Fabric Notebooks can output this as the pipeline activity result
mssparkutils.notebook.exit(json.dumps(summary))
