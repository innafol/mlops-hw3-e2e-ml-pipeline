import json, os
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task, Param

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.helpers import (
    build_run_config,
    prepare_run_dir,
    run_agent_batch,
    run_swebench_eval,
    collect_metrics,
    build_manifest,
    log_mlflow_run,
    upload_run_to_s3
)


# DAG definition
@dag(
    dag_id="evaluate_agent_local",
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
def evaluate_agent_local():

    @task
    def prepare_run(params, dag_run) -> dict:
        run_config = build_run_config(params, dag_run.run_id)
        run_dir = prepare_run_dir(run_config)
        run_config["run_dir"] = str(run_dir)
        return run_config

    @task
    def run_agent(run_config: dict) -> str:
        preds_path = run_agent_batch(run_config)
        return str(preds_path)

    @task
    def run_eval(run_config: dict, preds_path: str) -> str:
        eval_dir = run_swebench_eval(run_config, Path(preds_path))
        return str(eval_dir)

    @task
    def summarize_and_log(run_config: dict, eval_dir: str, params) -> None:
        current_run_id = run_config["run_id"]
        run_dir = Path(run_config["run_dir"])

        metrics = collect_metrics(Path(eval_dir))
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
                s3_uri=None
                manifest["s3_uri"] = s3_uri
                with open(run_dir / "manifest.json", "w") as f:
                    json.dump(manifest, f, indent=2)

        mlflow_run_id = log_mlflow_run(run_config, metrics, str(run_dir), s3_uri)

        return None


    # Execution Pipeline
    run_config = prepare_run()
    preds_path = run_agent(run_config) 
    eval_dir = run_eval(run_config, preds_path)
    summarize_and_log(run_config, eval_dir)

evaluate_agent_local()