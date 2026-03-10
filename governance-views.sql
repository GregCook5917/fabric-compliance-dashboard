-- =============================================================================
-- GOVERNANCE VIEWS FOR POWER BI
-- Run these once in your Azure SQL Database.
-- Power BI connects to these views — not the raw tables directly.
-- Schema: governance
-- =============================================================================


-- -----------------------------------------------------------------------------
-- vw_violation_summary
-- One row per violating item with all context Power BI needs.
-- Use this as the main fact table in Power BI.
-- -----------------------------------------------------------------------------
CREATE OR ALTER VIEW governance.vw_violation_summary AS
SELECT
    v.item_display_name                                     AS [Item Name],
    v.item_type                                             AS [Item Type],
    v.git_sync_state                                        AS [Git Sync State],
    v.severity                                              AS [Severity],

    -- Violation type flags (use in Power BI slicers)
    CASE WHEN v.v1_uncommitted_in_dev    = 1 THEN 'Yes' ELSE 'No' END AS [Uncommitted in Dev],
    CASE WHEN v.v2_promoted_while_dirty  = 1 THEN 'Yes' ELSE 'No' END AS [Promoted While Dirty],
    CASE WHEN v.v3_modified_after_commit = 1 THEN 'Yes' ELSE 'No' END AS [Modified After Commit],

    v.last_modified_by                                      AS [Last Modified By],
    v.last_modified_at                                      AS [Last Modified At],
    v.last_commit_at                                        AS [Last Committed At],
    v.last_commit_by                                        AS [Last Committed By],
    v.last_commit_msg                                       AS [Last Commit Message],

    -- Drift expressed in readable buckets
    CASE
        WHEN v.drift_hours IS NULL      THEN 'Unknown'
        WHEN v.drift_hours < 1          THEN '< 1 hour'
        WHEN v.drift_hours < 4          THEN '1 – 4 hours'
        WHEN v.drift_hours < 24         THEN '4 – 24 hours'
        WHEN v.drift_hours < 72         THEN '1 – 3 days'
        ELSE                                 '3+ days'
    END                                                     AS [Drift Window],
    ROUND(v.drift_hours, 1)                                 AS [Drift Hours],

    v.last_promoted_at                                      AS [Last Promoted At],
    v.promoted_to_stage                                     AS [Promoted To],
    v.promoted_from_stage                                   AS [Promoted From],
    v.promoted_by                                           AS [Promoted By],

    v.scan_timestamp                                        AS [Last Scan At]

FROM governance.fact_violations_current v
WHERE v.is_violation = 1;
GO


-- -----------------------------------------------------------------------------
-- vw_git_health_summary
-- Workspace-level health metrics. Use for KPI cards in Power BI.
-- -----------------------------------------------------------------------------
CREATE OR ALTER VIEW governance.vw_git_health_summary AS
SELECT
    COUNT(*)                                                AS [Total Items],
    SUM(CASE WHEN is_uncommitted = 1 THEN 1 ELSE 0 END)    AS [Uncommitted Items],
    SUM(CASE WHEN is_uncommitted = 0 THEN 1 ELSE 0 END)    AS [Committed Items],

    ROUND(
        100.0 * SUM(CASE WHEN is_uncommitted = 0 THEN 1 ELSE 0 END)
              / NULLIF(COUNT(*), 0),
        1
    )                                                       AS [Git Sync Rate %],

    scan_timestamp                                          AS [Last Scan At]

FROM governance.fact_git_status_current;
GO


-- -----------------------------------------------------------------------------
-- vw_violations_by_person
-- Aggregates violations per user. Use for the offender leaderboard visual.
-- -----------------------------------------------------------------------------
CREATE OR ALTER VIEW governance.vw_violations_by_person AS
SELECT
    COALESCE(v.last_modified_by, 'Unknown')                 AS [Developer],

    COUNT(*)                                                AS [Total Violations],
    SUM(CASE WHEN v.v2_promoted_while_dirty  = 1 THEN 1 ELSE 0 END)
                                                            AS [Promoted While Dirty],
    SUM(CASE WHEN v.v1_uncommitted_in_dev    = 1 THEN 1 ELSE 0 END)
                                                            AS [Uncommitted in Dev],
    SUM(CASE WHEN v.severity = 'HIGH'        THEN 1 ELSE 0 END)
                                                            AS [High Severity],

    MAX(v.last_modified_at)                                 AS [Most Recent Violation],
    MAX(v.scan_timestamp)                                   AS [Last Scan At]

FROM governance.fact_violations_current v
WHERE v.is_violation = 1
GROUP BY v.last_modified_by;
GO


-- -----------------------------------------------------------------------------
-- vw_deployment_ops_with_status
-- Joins deployments to current violation state so Power BI can show
-- which recent promotions were "dirty" at time of deploy.
-- -----------------------------------------------------------------------------
CREATE OR ALTER VIEW governance.vw_deployment_ops_with_status AS
SELECT
    d.operation_id                                          AS [Operation ID],
    d.item_display_name                                     AS [Item Name],
    d.item_type                                             AS [Item Type],
    d.source_stage                                          AS [From Stage],
    d.target_stage                                          AS [To Stage],
    d.operation_status                                      AS [Status],
    d.triggered_by                                          AS [Deployed By],
    d.created_at                                            AS [Deployed At],
    d.completed_at                                          AS [Completed At],

    -- Flag if this item currently has a promoted-while-dirty violation
    CASE WHEN v.v2_promoted_while_dirty = 1 THEN 'Yes' ELSE 'No' END
                                                            AS [Was Dirty at Promotion],
    v.severity                                              AS [Current Severity],
    v.git_sync_state                                        AS [Current Git State]

FROM governance.fact_deployment_ops_current d
LEFT JOIN governance.fact_violations_current v
       ON LOWER(TRIM(d.item_display_name)) = LOWER(TRIM(v.item_display_name));
GO


-- -----------------------------------------------------------------------------
-- vw_item_type_breakdown
-- Violation counts by item type. Use for a donut/bar chart in Power BI.
-- -----------------------------------------------------------------------------
CREATE OR ALTER VIEW governance.vw_item_type_breakdown AS
SELECT
    item_type                                               AS [Item Type],
    COUNT(*)                                                AS [Violation Count],
    SUM(CASE WHEN severity = 'HIGH'   THEN 1 ELSE 0 END)   AS [High],
    SUM(CASE WHEN severity = 'MEDIUM' THEN 1 ELSE 0 END)   AS [Medium],
    SUM(CASE WHEN severity = 'LOW'    THEN 1 ELSE 0 END)   AS [Low],
    MAX(scan_timestamp)                                     AS [Last Scan At]
FROM governance.fact_violations_current
WHERE is_violation = 1
GROUP BY item_type;
GO
