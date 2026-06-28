set -euo pipefail

export AIRFLOW_HOME=~/airflow
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=false

mkdir -p $AIRFLOW_HOME

echo '{"admin": "admin"}' > $AIRFLOW_HOME/simple_auth_manager_passwords.json.generated
export PYTHONPATH=$(pwd)/.venv/lib/python3.12/site-packages:${PYTHONPATH:-}

set -a
source $(pwd)/.env
set +a

uv tool run apache-airflow standalone
