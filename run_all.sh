#!/usr/bin/env bash
# Reproduces every experiment. Takes about 45-60 minutes. Needs docker access (sudo).
set -e
cd "$(dirname "$0")"
source venv/bin/activate
source env.sh
sudo docker compose up -d
sleep 15
rm -f results/results.jsonl results/experiment_summary.csv
E="python experiments/run_experiment.py"

# Task 4.2: partitions x consumers (same segment, same rate)
$E --id P1 --partitions 1 --slots 1
$E --id P2 --partitions 4 --slots 1
$E --id P3 --partitions 4 --slots 4
$E --id P4 --partitions 4 --slots 6

# Task 4.3: partition key
$E --id K1 --partitions 4 --slots 4 --key-mode pickup_zone
$E --id K2 --partitions 4 --slots 4 --key-mode round_robin

# Task 4.4: producer rate (consumer capacity limited identically in all three runs)
$E --id R-low    --rate 300  --cap 600
$E --id R-medium --rate 550  --cap 600
$E --id R-high   --rate 3000 --cap 600

# Task 5.1: late events (lateness 360 min = default, 30 min = strict)
$E --id L1-lateness360
$E --id L2-lateness30 --lateness-min 30

# Task 5.2: Stage 2 restart, with and without state snapshot
$E --id F1-restart-snapshot   --kill-stage2-at 40 --down-for 25
$E --id F2-restart-nosnapshot --kill-stage2-at 40 --down-for 25 --no-snapshot

echo "done: results/experiment_summary.csv and results/results.jsonl"