set -e

SPLIT=${1:-test}
SUBSET=${2:-verified}
WORKERS=${3:-5}
MODEL=${4:-nebius/moonshotai/Kimi-K2.6}
TASK_SLICE=${5:-0:3}
OUTPUT_DIR=${6:-trajectories}

MSWEA_COST_TRACKING='ignore_errors' mini-extra swebench \
    --subset "$SUBSET" \
    --split "$SPLIT" \
    --model "$MODEL" \
    --slice "$TASK_SLICE" \
    --workers "$WORKERS" \
    -o "$OUTPUT_DIR"
