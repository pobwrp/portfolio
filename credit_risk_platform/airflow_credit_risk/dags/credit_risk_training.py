"""Minimal Airflow DAG that orchestrates a two-step Spark ML pipeline."""

from datetime import timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context
from airflow.utils.dates import days_ago

from airflow_credit_risk.utils.spark_command import build_spark_submit

DAG_ID = "credit_risk_training"

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": days_ago(3),
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
}


@dag(
    dag_id=DAG_ID,
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["portfolio", "credit-risk"],
)
def credit_risk_training():
    @task(task_id="tag_run")
    def tag_run(ds: str) -> str:
        return f"{DAG_ID}_{ds}"

    @task(task_id="feature_engineering_config")
    def feature_engineering_config() -> str:
        context = get_current_context()
        return build_spark_submit("feature_engineering", "feature_engineering", **context)

    @task(task_id="model_training_config")
    def model_training_config() -> str:
        context = get_current_context()
        return build_spark_submit("credit_risk_model_training", "model_training", **context)

    feature_engineering = BashOperator(
        task_id="feature_engineering",
        bash_command="{{ ti.xcom_pull('feature_engineering_config') }}",
    )

    model_training = BashOperator(
        task_id="model_training",
        bash_command="{{ ti.xcom_pull('model_training_config') }}",
    )

    tag_run() >> feature_engineering_config() >> feature_engineering >> model_training_config() >> model_training


dag = credit_risk_training()
