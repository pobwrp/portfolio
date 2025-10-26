#!/usr/bin/env bash
# Bootstrap script for the local Airflow developer environment.

set -euo pipefail

AIRFLOW_HOME="${AIRFLOW_HOME:-$(pwd)/local_airflow}"
export AIRFLOW_HOME

mkdir -p "$AIRFLOW_HOME/logs"

airflow db upgrade
airflow scheduler --daemon
airflow webserver --daemon --port 8099
