#!/usr/bin/env bash
# Run from any directory. Override these budgets only when Docker has headroom.
set -euo pipefail
cd "$(dirname "$0")/.."
exec docker compose exec -T spark-master spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory "${BRONZE_DRIVER_MEMORY:-768m}" \
  --executor-memory "${BRONZE_EXECUTOR_MEMORY:-512m}" \
  --executor-cores 1 \
  --total-executor-cores 1 \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.executor.instances=1 \
  /opt/bitnami/spark/scripts/load_bronze.py "$@"
