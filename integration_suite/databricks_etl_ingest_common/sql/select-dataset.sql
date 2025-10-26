SELECT
    customer_id,
    snapshot_ts,
    credit_score,
    delinquency_risk
FROM mart.customer_snapshot
WHERE data_dt = '${data_dt}'
