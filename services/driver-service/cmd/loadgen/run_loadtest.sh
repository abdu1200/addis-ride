#!/usr/bin/env bash
# Run from the driver-service folder: bash cmd/loadgen/run_loadtest.sh
# 3 driver counts x 2 shard settings x 60s = about 6 minutes. Close Chrome first.
set -e

mkdir -p results
go build -o loadgen_bin ./cmd/loadgen

for drivers in 10000 50000 100000; do
  for shards in 1 256; do
    echo "running drivers=$drivers shards=$shards ..."
    ./loadgen_bin -drivers=$drivers -shards=$shards -interval=4s -duration=60s -json >> results/go_ingestion.jsonl
  done
done

rm loadgen_bin
echo "done -> results/go_ingestion.jsonl"