import json, os
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task, Param
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.helpers import (
    build_run_config,
    prepare_run_dir,
    collect_metrics,
    build_manifest,
    log_mlflow_run,
    upload_run_to_s3,
    fix_preds_location,
    fix_report_location,
    PROJECT_ROOT
)

RUNS_DIR = PROJECT_ROOT / "runs"
HOST_PROJECT_ROOT = "/home/inna/mlops-hw3-e2e-ml-pipeline"

COMMON_ENV = {
    "NEBIUS_API_KEY": os.environ.get("NEBIUS_API_KEY", ""),
    "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
    "S3_ENDPOINT_URL": os.environ.get("S3_ENDPOINT_URL", ""),
    "S3_BUCKET": os.environ.get("S3_BUCKET", ""),
    "MLFLOW_URL": os.environ.get("MLFLOW_URL", ""),
    "MSWEA_COST_TRACKING": "ignore_errors",
}

COMMON_DOCKER = dict(
    image="mlops-hw3-airflow",
    mounts=[
        Mount(
            target="/mlops-hw3-e2e-ml-pipeline/runs",
            source=f"{HOST_PROJECT_ROOT}/runs",
            type="bind"
        ),
        Mount(
            target="/var/run/docker.sock",
            source="/var/run/docker.sock",
            type="bind",
        ),
    ],
    environment=COMMON_ENV,
    docker_url="unix://var/run/docker.sock",
    network_mode="host",
    auto_remove="success",
    working_dir="/mlops-hw3-e2e-ml-pipeline",
    mount_tmp_dir=False,
)

@dag(
    dag_id="evaluate_agent",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "split": Param("test", type="string"),
        "subset": Param("verified", type="string"),
        "workers": Param(2, type="integer"),
        "model": Param("nebius/moonshotai/Kimi-K2.6", type="string"),
        "task_slice": Param("0:3", type="string"),
        "cost_limit": Param(1.0, type="number"),
        "use_s3": Param(True, type="boolean", description="Upload artifacts to S3"),
    },
)
def evaluate_agent():

    @task
    def prepare_run(params, dag_run) -> str:
        current_run_id = dag_run.run_id
        run_dir = RUNS_DIR / current_run_id
        run_config = build_run_config(params, current_run_id)
        prepare_run_dir(run_dir, run_config)
        return current_run_id

    @task
    def summarize_and_log(current_run_id: str, params, dag_run) -> None:
        run_dir = RUNS_DIR / current_run_id
        run_config = build_run_config(params, current_run_id)
        eval_dir = run_dir / "run-eval"

        fix_report_location(run_dir, current_run_id)
        metrics = collect_metrics(eval_dir)
        s3_uri = f"s3://{os.environ.get('S3_BUCKET', '')}/{current_run_id}" if params["use_s3"] else None

        with open(run_dir / "metrics.json", "w") as f:
            json.dump(metrics, f, indent=2)

        manifest = build_manifest(current_run_id, run_dir, s3_uri)
        with open(run_dir / "manifest.json", "w") as f:
            json.dump(manifest, f, indent=2)

        if params["use_s3"]:
            try:
                upload_run_to_s3(run_dir, current_run_id)
            except Exception as e:
                print(f"S3 upload failed: {e}")
                s3_uri = None
                manifest["s3_uri"] = s3_uri
                with open(run_dir / "manifest.json", "w") as f:
                    json.dump(manifest, f, indent=2)

        log_mlflow_run(run_config, metrics, str(run_dir), s3_uri)


    @task
    def fix_preds(current_run_id: str) -> None:
        fix_preds_location(RUNS_DIR / current_run_id)


    # Pipeline
    current_run_id = prepare_run()

    run_agent = DockerOperator(
        task_id="run_agent",
        command="""bash scripts/mini-swe-bench-batch.sh \
            {{ params.split }} \
            {{ params.subset }} \
            {{ params.workers }} \
            {{ params.model }} \
            {{ params.task_slice }} \
            runs/{{ task_instance.xcom_pull(task_ids='prepare_run') }}/run-agent/trajectories""",
        **COMMON_DOCKER,
    )

    run_eval = DockerOperator(
        task_id="run_eval",
        command="""bash -c 'cd runs/{{ task_instance.xcom_pull(task_ids="prepare_run") }}/run-eval && \
            bash /mlops-hw3-e2e-ml-pipeline/scripts/swe-bench-eval.sh \
            ../run-agent/preds.json \
            {{ params.workers }} \
            {{ task_instance.xcom_pull(task_ids="prepare_run") }} \
            reports'""",
        **COMMON_DOCKER,
    )

    fixed = fix_preds(current_run_id)
    summarize = summarize_and_log(current_run_id)

    current_run_id >> run_agent >> fixed >> run_eval >> summarize

evaluate_agent()