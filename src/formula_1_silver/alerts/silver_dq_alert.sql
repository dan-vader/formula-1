WITH issues AS (
    SELECT 'error' AS severity, 'row_balance' AS check_id, table_name AS scope,
           CONCAT('rows_in=', rows_in, ' accounted=',
                  rows_out + rows_quarantined + rows_deduplicated + rows_filtered_out) AS detail
    FROM dbr_dev_ua5816bd.formula_1_silver.silver_dq_metrics
    WHERE NOT reconciles

    UNION ALL
    SELECT 'error', check_id, scope_key,
           CONCAT(left_name, '=', left_value, ' vs ', right_name, '=', right_value)
    FROM dbr_dev_ua5816bd.formula_1_silver.silver_dq_reconciliation
    WHERE NOT passed

    UNION ALL
    SELECT 'warn', 'quarantine_share', table_name,
           CONCAT('quarantined ', rows_quarantined, ' of ', rows_in)
    FROM dbr_dev_ua5816bd.formula_1_silver.silver_dq_metrics
    WHERE rows_in > 0 AND rows_quarantined > 0.05 * rows_in

    UNION ALL
    SELECT 'warn', 'schema_drift', source_table,
           CONCAT('new: ', array_join(unexpected_keys, ','), '; missing: ', array_join(missing_keys, ','))
    FROM dbr_dev_ua5816bd.formula_1_silver.silver_schema_drift
    WHERE has_drift
)
SELECT
    count_if(severity = 'error') AS errors,
    count_if(severity = 'warn') AS warnings,
    array_join(collect_list(CASE WHEN severity = 'error' THEN CONCAT(check_id, ':', scope, ' (', detail, ')') END), '; ') AS error_details
FROM issues