"""Example Airflow DAG wiring DataStageJobExecutor into a PythonOperator."""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datastage_job_runner.datastage_job_executor import DataStageJobExecutor

with DAG(
    dag_id="datastage_job_runner_demo",
    schedule=None,
    catchup=False,
    start_date=days_ago(1),
    tags=["datastage", "portfolio"],
) as dag:
    run_datastage = PythonOperator(
        task_id="run_datastage",
        python_callable=DataStageJobExecutor(
            conn_id="datastage",
            project_name="Data_Integration",
            job_name="SAMPLE_JOB",
            params={"DATA_DT": "{{ ds }}"},
        ).proceed,
    )

run_datastage
