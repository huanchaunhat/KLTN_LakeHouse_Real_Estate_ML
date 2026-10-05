#!/bin/bash

mkdir -p bronze_load_logs

runs=(
  #"2026-08-19 legacy_20260819_100631"
  #"2026-08-24 legacy_20260824_143001"
  #"2026-09-07 legacy_20260907_121541"
  #"2026-09-08 legacy_20260908_145029"
  #"2026-09-09 legacy_20260909_162534"
  #"2026-09-10 legacy_20260910_131712"
  #"2026-09-11 legacy_20260911_102620"
  #"2026-09-12 legacy_20260912_112831"
  #"2026-09-13 legacy_20260913_133956"
  #"2026-09-14 legacy_20260914_134145"
  #"2026-09-15 legacy_20260915_150640"
  #"2026-09-16 legacy_20260916_165453"
  #"2026-09-17 legacy_20260917_155154"
  "2026-09-18 20260918_073406"
  "2026-09-19 20260919_065821"
)

for item in "${runs[@]}"; do
  read -r crawl_date run_id <<< "$item"

  log_file="bronze_load_logs/${run_id}.log"

  echo "========================================"
  echo "Loading $crawl_date / $run_id"
  echo "========================================"

  docker compose exec spark-master \
    spark-submit \
    --master spark://spark-master:7077 \
    --driver-memory 768m \
    --executor-memory 768m \
    --executor-cores 1 \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.cores.max=2 \
    /opt/bitnami/spark/scripts/load_bronze.py \
    --crawl-date "$crawl_date" \
    --run-id "$run_id" \
    2>&1 | tee "$log_file"

  status=${PIPESTATUS[0]}

  if [ $status -ne 0 ]; then
    echo "FAILED: $run_id"
    exit 1
  fi

  echo "SUCCESS: $run_id"
done

echo "ALL LEGACY RUNS COMPLETED"