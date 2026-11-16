#!/usr/bin/env bash
# Run from the driver-service folder: bash cmd/matchload/run_matchload.sh
# 3 worker counts x 4 request rates x 20s = about 5 minutes. Close Chrome first.
set -e

mkdir -p results
go build -o matchload_bin ./cmd/matchload

for workers in 2 8 32; do
  for rate in 1000 4000 8000 16000; do
    echo "running workers=$workers rate=$rate ..."
    ./matchload_bin -workers=$workers -rate=$rate -queue=1024 -drivers=100000 -shards=256 -duration=20s -json >> results/go_matching.jsonl
  done
done

rm matchload_bin
echo "done -> results/go_matching.jsonl"