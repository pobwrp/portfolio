# Spark Modeling Playbook

Refined PySpark job that trains a random-forest classifier for credit
risk scoring. The module balances classes, evaluates AUC/precision/recall,
and writes both model artifacts and metrics so orchestration layers can
track model health.

## Key Concepts
- Modular helper functions (`filter_recent_observations`, `rebalance`, `evaluate`).
- Configurable entry point with CLI arguments consumed by Airflow.
- Safe filesystem writes using `pathlib` to avoid hard-coded cluster paths.

## Running the Job
```bash
spark-submit credit_risk_platform/spark-modeling/credit_risk_model_training.py \
  --feature_store_path /data/feature_store/parquet \
  --model_registry_path /tmp/registry \
  --metrics_path /tmp/metrics \
  --training_window_months 6 \
  --sampling_ratio 1.5 \
  --run_tag manual__20240101
```

Sample feature store schema:
- `snapshot_date` (`date`)
- `label_default` (`int`)
- Numeric features such as `debt_to_income`, `utilisation_ratio`
- Categorical features including `customer_segment`, `region`, `occupation`

Replace the placeholders with your dataset paths before execution.
