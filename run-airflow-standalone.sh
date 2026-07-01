#!/bin/bash
set -euo pipefail

export AIRFLOW_HOME=~/airflow
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=false

mkdir -p "$AIRFLOW_HOME"

echo '{"admin":"admin"}' > "$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated"

mkdir -p runs
sudo chown -R "$USER:$USER" runs/
sudo chmod 666 /var/run/docker.sock

set -a
source "$(pwd)/.env"
set +a

source .venv/bin/activate

airflow standalone