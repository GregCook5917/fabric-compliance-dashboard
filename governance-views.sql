-- =============================================================================
-- GOVERNANCE VIEWS FOR POWER BI  v2
-- Run these once against your Azure SQL Database.
-- Updates from v1:
--   • CRITICAL severity tier for direct Prod edits
--   • v4_direct_edit_test / v4_direct_edit_prod columns
--   • vw_direct_edits_downstream  — new view for Test/Prod bypass violations
--   • vw_pending_promotions       — new informational view (healthy queue)
--   • vw_violations_by_person     — now includes direct edit counts
--   • vw_git_health_summary       — now includes pending promotion count
-- =============================================================================


-- -----------------------------------------------------------------------------
-- vw_violation_summary
-- One row per violating item. Primary fact table for Power BI.
-- Severity order for conditional formatting: CRITICAL > HIGH > MEDIUM > LOW
-- -----------------------------------------------------------------------------
CREATE OR ALTER VIEW governance.vw_violation_summary AS
SELECT
    v.item_display_name                                         AS [Item Name],
    v.item_type                                                 AS [Item Type],
    v.git_sync_state                                            AS [Git Sync State],
    v.severity                                                  AS [Severity],

    -- Severity sort column for Power BI conditional formatting
    CASE v.severity
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH'     THEN 2
        WHEN 'MEDIUM'   THEN 3
        WHEN 'LOW'      THEN 4
        ELSE                 5
    END                                                         AS [Severity Order],

    -- Violation type flags
    CASE WHEN v.v1_uncommitted_in_dev    = 1 THEN 'Yes' ELSE 'No' END AS [Uncommitted in Dev],
    CASE WHEN v.v2_promoted_while_dirty  = 1 THEN 'Yes' ELSE 'No' END AS [Promoted While Dirty],
    CASE WHEN v.v3_modified_after_commit = 1 THEN 'Yes' ELSE 'No' END AS [Modified After Commit],
    CASE WHEN v.v4_direct_edit_test      = 1 THEN 'Yes' ELSE 'No' END AS [Direct Edit in Test],
    CASE WHEN v.v4_direct_edit_prod      = 1 THEN 'Yes' ELSE 'No' END AS [Direct Edit in Prod],

    -- Dev drift context
    v.last_modified_by                                          AS [Last Modified By (Dev)],
    v.last_modified_at                                          AS [Last Modified At (Dev)],
    v.last_commit_at                                            AS [Last Committed At],
    v.last_commit_by                                            AS [Last Committed By],
    v.last_commit_msg                                           AS [Last Commit Message],

    CASE
        WHEN v.drift_hours IS NULL  THEN 'Unknown'
        WHEN v.drift_hours < 1      THEN '< 1 hour'
        WHEN v.drift_hours < 4      THEN '1 – 4 hours'
        WHEN v.drift_hours < 24     THEN '4 – 24 hours'
        WHEN v.drift_hours < 72     THEN '1 – 3 days'
        ELSE                             '3+ days'
    END                                                         AS [Drift Window],
    ROUND(v.drift_hours, 1)                                     AS [Drift Hours],

    -- Deployment context
    v.last_promoted_at                                          AS [Last Promoted At],
    v.promoted_to_stage                                         AS [Promoted To],
    v.promoted_from_stage                                       AS [Promoted From],
    v.promoted_by                                               AS [Promoted By],

    -- Test direct edit detail
    v.v4_test_modified_at                                       AS [Test Direct Edit At],
    v.v4_test_modified_by                                       AS [Test Direct Edit By],
    v.v4_last_deployed_test                                     AS [Last Deployed to Test],

    -- Prod direct edit detail
    v.v4_prod_modified_at                                       AS [Prod Direct Edit At],
    v.v4_prod_modified_by                                       AS [Prod Direct Edit By],
    v.v4_last_deployed_prod                                     AS [Last Deployed to Prod],

    v.scan_timestamp                                            AS [Last Scan At]

FROM governance.fact_violations_current v
WHERE v.is_violation = 1;
GO


-- -----------------------------------------------------------------------------
-- vw_direct_edits_downstream
-- NEW — focused view on Test and Prod bypass violations only.
-- Use this for the "Direct Workspace Edits" page in Power BI.
-- Each row is one item that was edited directly in Test or Prod.
-- An item can appear TWICE if edited in both stages.
-- -----------------------------------------------------------------------------
CREATE OR ALTER VIEW governance.vw_direct_edits_downstream AS

    -- Test direct edits
    SELECT
        v.item_display_name                                     AS [Item Name],
        v.item_type                                             AS [Item Type],
        'Test'                                                  AS [Affected Stage],
        'MEDIUM'                                                AS [Severity],
        v.v4_test_modified_by                                   AS [Edited By],
        v.v4_test_modified_at                                   AS [Edited At],
        v.v4_last_deployed_test                                 AS [Last Deployed to Stage],
        CASE
            WHEN v.v4_last_deployed_test IS NULL
                THEN 'Never deployed — item may have been created directly in Test'
            ELSE
                CONCAT(
                    'Modified ',
                    DATEDIFF(HOUR, v.v4_last_deployed_test, v.v4_test_modified_at),
                    ' hour(s) after last deployment'
                )
        END                                                     AS [Context],
        v.scan_timestamp                                        AS [Last Scan At]
    FROM governance.fact_violations_current v
    WHERE v.v4_direct_edit_test = 1

UNION ALL

    -- Prod direct edits (CRITICAL)
    SELECT
        v.item_display_name                                     AS [Item Name],
        v.item_type                                             AS [Item Type],
        'Prod'                                                  AS [Affected Stage],
        'CRITICAL'                                              AS [Severity],
        v.v4_prod_modified_by                                   AS [Edited By],
        v.v4_prod_modified_at                                   AS [Edited At],
        v.v4_last_deployed_prod                                 AS [Last Deployed to Stage],
        CASE
            WHEN v.v4_last_deployed_prod IS NULL
                THEN 'Never deployed — item may have been created directly in Prod'
            ELSE
                CONCAT(
                    'Modified ',
                    DATEDIFF(HOUR, v.v4_last_deployed_prod, v.v4_prod_modified_at),
                    ' hour(s) after last deployment'
                )
        END                                                     AS [Context],
        v.scan_timestamp                                        AS [Last Scan At]
    FROM governance.fact_violations_current v
    WHERE v.v4_direct_edit_prod = 1;
GO


-- -----------------------------------------------------------------------------
-- vw_pending_promotions
-- NEW — informational view. Items committed in Dev but not yet promoted.
-- These are NOT violations — this is the correct workflow in progress.
-- Use this to give the team a "ready to deploy" queue in Power BI.
-- -----------------------------------------------------------------------------
CREATE OR ALTER VIEW governance.vw_pending_promotions AS
SELECT
    v.item_display_name                                         AS [Item Name],
    v.item_type                                                 AS [Item Type],
    v.last_commit_by                                            AS [Committed By],
    v.last_commit_at                                            AS [Committed At],
    v.last_commit_msg                                           AS [Commit Message],
    v.last_promoted_at                                          AS [Last Promoted At],
    v.promoted_to_stage                                         AS [Last Promoted To],

    -- How long has the committed change been sitting undeployed?
    CASE
        WHEN v.last_promoted_at IS NULL
            THEN 'Never promoted'
        ELSE
            CONCAT(
                DATEDIFF(HOUR, v.last_commit_at, GETUTCDATE()),
                ' hour(s) since commit'
            )
    END                                                         AS [Waiting Since],

    DATEDIFF(HOUR, v.last_commit_at, GETUTCDATE())              AS [Hours Since Commit],

    v.scan_timestamp                                            AS [Last Scan At]

FROM governance.fact_violations_current v
WHERE v.pending_promotion = 1
  AND v.is_violation       = 0;  -- Exclude items that are ALSO violations
GO


-- -----------------------------------------------------------------------------
-- vw_git_health_summary
-- KPI cards for the Power BI header row.
-- Now includes CRITICAL count and pending promotion count.
-- -----------------------------------------------------------------------------
CREATE OR ALTER VIEW governance.vw_git_health_summary AS
SELECT
    COUNT(*)                                                        AS [Total Items Tracked],
    SUM(CASE WHEN is_uncommitted = 1 THEN 1 ELSE 0 END)            AS [Uncommitted in Dev],
    SUM(CASE WHEN is_uncommitted = 0 THEN 1 ELSE 0 END)            AS [Committed in Dev],
    ROUND(
        100.0 * SUM(CASE WHEN is_uncommitted = 0 THEN 1 ELSE 0 END)
              / NULLIF(COUNT(*), 0), 1
    )                                                               AS [Git Sync Rate %],

    -- Cross-join to violation table for severity counts
    (SELECT COUNT(*) FROM governance.fact_violations_current
     WHERE is_violation = 1 AND severity = 'CRITICAL')             AS [Critical Violations],

    (SELECT COUNT(*) FROM governance.fact_violations_current
     WHERE is_violation = 1 AND severity = 'HIGH')                 AS [High Violations],

    (SELECT COUNT(*) FROM governance.fact_violations_current
     WHERE is_violation = 1)                                        AS [Total Violations],

    (SELECT COUNT(*) FROM governance.fact_violations_current
     WHERE v4_direct_edit_prod = 1)                                AS [Direct Prod Edits],

    (SELECT COUNT(*) FROM governance.fact_violations_current
     WHERE pending_promotion = 1 AND is_violation = 0)             AS [Pending Promotions],

    MAX(scan_timestamp)                                             AS [Last Scan At]

FROM governance.fact_git_status_current;
GO


-- -----------------------------------------------------------------------------
-- vw_violations_by_person
-- Offender leaderboard. Now breaks out direct edit counts separately
-- so you can distinguish "forgot to commit" from "went around the process".
-- -----------------------------------------------------------------------------
CREATE OR ALTER VIEW governance.vw_violations_by_person AS

-- Dev violations (attributed to last_modified_by)
SELECT
    COALESCE(v.last_modified_by, 'Unknown')     AS [Developer],
    COUNT(*)                                     AS [Total Violations],
    SUM(CASE WHEN v.v2_promoted_while_dirty  = 1 THEN 1 ELSE 0 END) AS [Promoted While Dirty],
    SUM(CASE WHEN v.v1_uncommitted_in_dev    = 1 THEN 1 ELSE 0 END) AS [Uncommitted in Dev],
    0                                            AS [Direct Test Edits],
    0                                            AS [Direct Prod Edits],
    SUM(CASE WHEN v.severity = 'CRITICAL'    THEN 1 ELSE 0 END)     AS [Critical],
    SUM(CASE WHEN v.severity = 'HIGH'        THEN 1 ELSE 0 END)     AS [High],
    MAX(v.last_modified_at)                      AS [Most Recent Violation],
    MAX(v.scan_timestamp)                        AS [Last Scan At]
FROM governance.fact_violations_current v
WHERE v.is_violation = 1
  AND (v.v1_uncommitted_in_dev = 1
    OR v.v2_promoted_while_dirty = 1
    OR v.v3_modified_after_commit = 1)
GROUP BY v.last_modified_by

UNION ALL

-- Test direct edits (attributed to v4_test_modified_by)
SELECT
    COALESCE(v.v4_test_modified_by, 'Unknown')  AS [Developer],
    COUNT(*)                                     AS [Total Violations],
    0, 0,
    COUNT(*)                                     AS [Direct Test Edits],
    0                                            AS [Direct Prod Edits],
    0                                            AS [Critical],
    COUNT(*)                                     AS [High],
    MAX(v.v4_test_modified_at)                   AS [Most Recent Violation],
    MAX(v.scan_timestamp)                        AS [Last Scan At]
FROM governance.fact_violations_current v
WHERE v.v4_direct_edit_test = 1
GROUP BY v.v4_test_modified_by

UNION ALL

-- Prod direct edits (attributed to v4_prod_modified_by) — CRITICAL
SELECT
    COALESCE(v.v4_prod_modified_by, 'Unknown')  AS [Developer],
    COUNT(*)                                     AS [Total Violations],
    0, 0, 0,
    COUNT(*)                                     AS [Direct Prod Edits],
    COUNT(*)                                     AS [Critical],
    0                                            AS [High],
    MAX(v.v4_prod_modified_at)                   AS [Most Recent Violation],
    MAX(v.scan_timestamp)                        AS [Last Scan At]
FROM governance.fact_violations_current v
WHERE v.v4_direct_edit_prod = 1
GROUP BY v.v4_prod_modified_by;
GO


-- -----------------------------------------------------------------------------
-- vw_deployment_ops_with_status
-- Recent promotions enriched with current violation state.
-- -----------------------------------------------------------------------------
CREATE OR ALTER VIEW governance.vw_deployment_ops_with_status AS
SELECT
    d.operation_id                                              AS [Operation ID],
    d.item_display_name                                         AS [Item Name],
    d.item_type                                                 AS [Item Type],
    d.source_stage                                              AS [From Stage],
    d.target_stage                                              AS [To Stage],
    d.operation_status                                          AS [Status],
    d.triggered_by                                              AS [Deployed By],
    d.created_at                                                AS [Deployed At],
    d.completed_at                                              AS [Completed At],
    CASE WHEN v.v2_promoted_while_dirty = 1
         THEN 'Yes' ELSE 'No' END                               AS [Was Dirty at Promotion],
    v.severity                                                  AS [Current Severity],
    v.git_sync_state                                            AS [Current Git State]
FROM governance.fact_deployment_ops_current d
LEFT JOIN governance.fact_violations_current v
       ON LOWER(TRIM(d.item_display_name)) = LOWER(TRIM(v.item_display_name));
GO


-- -----------------------------------------------------------------------------
-- vw_item_type_breakdown
-- Violation counts by item type. Donut / bar chart in Power BI.
-- -----------------------------------------------------------------------------
CREATE OR ALTER VIEW governance.vw_item_type_breakdown AS
SELECT
    item_type                                                   AS [Item Type],
    COUNT(*)                                                    AS [Violation Count],
    SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END)     AS [Critical],
    SUM(CASE WHEN severity = 'HIGH'     THEN 1 ELSE 0 END)     AS [High],
    SUM(CASE WHEN severity = 'MEDIUM'   THEN 1 ELSE 0 END)     AS [Medium],
    SUM(CASE WHEN severity = 'LOW'      THEN 1 ELSE 0 END)     AS [Low],
    MAX(scan_timestamp)                                         AS [Last Scan At]
FROM governance.fact_violations_current
WHERE is_violation = 1
GROUP BY item_type;
GO
