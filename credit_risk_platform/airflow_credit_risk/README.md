# Airflow Credit Risk Orchestration

This project demonstrates how recurring credit-risk model training and
feature engineering workflows were orchestrated in Airflow.

## Highlights
- Context-aware Spark submit command builder (`utils/spark_command.py`).
- Config-driven DAG design with separate tasks for feature engineering and model retraining.
- Developer helpers for spinning up a local Airflow instance (`scripts/start_airflow.sh`).

## File Map
- `dags/credit_risk_training.py` — Portfolio DAG that illustrates XCom-driven chaining of Spark jobs.
- `config/credit_risk_training/*.json` — Task-level Spark configuration with placeholders for runtime values.
- `utils/` — Reusable helpers exposed as Python packages for Airflow to import.
- `scripts/` — Local bootstrap scripts for scheduler/webserver lifecycle management.

## Running Locally
1. Install Airflow 2.x in a virtual environment.
2. Add `credit_risk_platform` to `PYTHONPATH` so the DAG can import `airflow_credit_risk`.
3. Run the bootstrap script:
   ```bash
   bash credit_risk_platform/airflow_credit_risk/scripts/start_airflow.sh
   ```
4. Trigger the DAG from the Airflow UI or via CLI:
   ```bash
   airflow dags trigger credit_risk_training
   ```

The Spark command helper returns a string, enabling seamless reuse of the
same Python code inside `BashOperator`, tests, or standalone scripts.
