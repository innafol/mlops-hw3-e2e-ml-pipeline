#!/bin/bash
set -e

PREDICTIONS_PATH=${1:-trajectories/preds.json}
WORKERS=${2:-5}
RUN_ID=${3:-test}
REPORT_DIR=${4:-.}

python -m swebench.harness.run_evaluation \
    --dataset_name princeton-nlp/SWE-bench_Verified \
    --predictions_path "$PREDICTIONS_PATH" \
    --max_workers "$WORKERS" \
    --run_id "$RUN_ID" \
    --report_dir "$REPORT_DIR"
