#!/usr/bin/env bash
# Stop background Airflow processes that were launched in daemon mode.

set -euo pipefail

pkill -f "airflow scheduler" || true
pkill -f "airflow webserver" || true
