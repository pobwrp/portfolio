"""Airflow DAG that triggers a Databricks notebook on an existing cluster."""

from airflow.decorators import dag
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.utils.dates import days_ago


@dag(schedule=None, start_date=days_ago(1), catchup=False, tags=["databricks", "portfolio"])
def run_databricks_notebook():
    DatabricksSubmitRunOperator(
        task_id="run_notebook",
        databricks_conn_id="databricks_portfolio",
        existing_cluster_id="1111-222222-sample",
        notebook_task={
            "notebook_path": "/Shared/portfolio-demo/print_job_params",
            "base_parameters": {"data_dt": "{{ ds }}"},
        },
    )


doc_dag = run_databricks_notebook()
