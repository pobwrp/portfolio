"""Utility helpers for constructing Spark submit commands.

This module mirrors the production pattern of loading Spark parameters
from task-specific configuration files and injecting DAG runtime context
variables before sending the command to ``BashOperator``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_ROOT = PROJECT_ROOT / "airflow_credit_risk" / "config"
SPARK_ROOT = PROJECT_ROOT / "spark-modeling"


def _render(value: Any, context_vars: Dict[str, str]) -> Any:
    """Recursively format strings within ``value`` using ``context_vars``."""
    if isinstance(value, str):
        return value.format_map(context_vars)
    if isinstance(value, dict):
        return {key: _render(inner, context_vars) for key, inner in value.items()}
    if isinstance(value, list):
        return [_render(item, context_vars) for item in value]
    return value


def _load_config(dag_name: str, task_name: str) -> Dict[str, Any]:
    cfg_path = CONFIG_ROOT / dag_name / f"{task_name}.json"
    with cfg_path.open() as fh:
        return json.load(fh)


def build_spark_submit(script_name: str, task_name: str, **context) -> str:
    """Compose the Spark submit command for the requested task.

    Parameters
    ----------
    script_name:
        Logical name of the PySpark entry point (without ``.py`` extension).
    task_name:
        Task identifier that maps to a JSON config file inside ``config/``.
    context:
        Airflow runtime context injected by ``PythonOperator``.
    """

    dag = context["dag"]
    dag_run = context.get("dag_run")
    ti = context.get("task_instance")

    context_vars = {
        "ds": context.get("ds"),
        "ds_nodash": context.get("ds_nodash"),
        "ts": context.get("ts"),
        "run_id": (dag_run.run_id if dag_run else getattr(ti, "run_id", "manual__01")),
    }

    config = _render(_load_config(dag.dag_id, task_name), context_vars)

    spark_cfg = config.get("spark", {})
    binary = spark_cfg.get("binary", "spark-submit")
    options = spark_cfg.get("options", [])

    script_root = config.get("script_root") or str(SPARK_ROOT)
    script_path = (PROJECT_ROOT / script_root).resolve() / f"{config.get('script', script_name)}.py"

    params = config.get("params", {})
    args = []
    for key, value in params.items():
        args.extend([f"--{key.replace('_', '-')}", str(value)])

    command = [binary, *options, str(script_path), *args]
    return " ".join(command)


__all__ = ["build_spark_submit"]
