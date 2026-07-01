import json
import os
import re
import shutil
import boto3
import time
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = PROJECT_ROOT / "runs"


def build_run_config(params: dict, airflow_run_id: str) -> dict:
    """Build a run configuration dict from Airflow params and current_run_id."""
    current_run_id = params.get("run_id") or airflow_run_id
    return {
        "airflow_run_id": airflow_run_id,
        "run_id": re.sub(r'[^a-zA-Z0-9_.-]', '-', current_run_id),     #cleaned run id for folder names to resolve docker issue
        "split": params["split"],
        "subset": params["subset"],
        "workers": params["workers"],
        "model": params["model"],
        "task_slice": params["task_slice"],
        "cost_limit": params["cost_limit"],
        "created_at": datetime.utcnow().isoformat(),
    }


def prepare_run_dir(run_config: dict) -> Path:
    """Create the run directory structure and write config.json."""
    current_run_id = run_config["run_id"]
    run_dir = RUNS_DIR / current_run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "run-agent").mkdir(exist_ok=True)
    (run_dir / "run-eval" / "logs").mkdir(parents=True, exist_ok=True)
    (run_dir / "run-eval" / "reports").mkdir(parents=True, exist_ok=True)
    with open(run_dir / "config.json", "w") as f:
        json.dump(run_config, f, indent=2)
    return run_dir


def fix_report_location(run_dir: Path, run_id: str) -> None:
    """Move generated report into reports/ subfolder if misplaced.
      This is a workaround for SWE-bench issue #449: report_dir CLI argument not working as expected in SWE-bench
      This resolves bug in make_run_report() in reporting.py which writes the report to cwd instead of report_dir.
    """
    eval_out_dir = run_dir / "run-eval"
    report_pattern = f"*.{run_id}.json"
    for report_file in eval_out_dir.glob(report_pattern):
        shutil.move(str(report_file), str(eval_out_dir / "reports" / report_file.name))
    return None


def collect_metrics(eval_dir: Path) -> dict:
    """Parse evaluation report and return key metrics dict."""
    import glob
    report_files = glob.glob(str(eval_dir / "reports" / "*.json"))
    if not report_files:
        report_files = glob.glob(str(eval_dir / "*.json"))
    if not report_files:
        return {}
    with open(report_files[0]) as f:
        report = json.load(f)
    return {
        "resolved_instances": report.get("resolved_instances", 0),
        "submitted_instances": report.get("submitted_instances", 0),
        "completed_instances": report.get("completed_instances", 0),
        "unresolved_instances": report.get("unresolved_instances", 0),
        "empty_patch_instances": report.get("empty_patch_instances", 0),
        "error_instances": report.get("error_instances", 0),
        "total_instances": report.get("total_instances", 0),
        "resolve_rate": report.get("resolved_instances", 0) / max(report.get("submitted_instances", 1), 1),
        "error_rate": report.get("error_instances", 0) / max(report.get("submitted_instances", 1), 1),
        "completion_rate": report.get("completed_instances", 0) / max(report.get("total_instances", 1), 1),
    }


def build_manifest(run_id: str, run_dir: Path, s3_uri: str = None) -> dict:
    """Build manifest dictionary pointing to all run artifacts."""
    return {
        "run_id": run_id,
        "config": "config.json",
        "metrics": "metrics.json",
        "predictions": "run-agent/preds.json",
        "trajectories": "run-agent/trajectories",
        "logs": "run-eval/logs",
        "reports": "run-eval/reports",
        "s3_uri": s3_uri,
    }


def log_mlflow_run(run_config: dict, metrics: dict, artifact_uri: str, s3_uri: str = None) -> str:
    """Log run config, metrics, and artifact path to MLflow."""
    import mlflow
    mlflow.set_tracking_uri(os.environ["MLFLOW_URL"])
    mlflow.set_experiment("evaluate_agent")

    with mlflow.start_run(run_name=run_config["run_id"]) as run:
        mlflow.log_params(run_config)
        mlflow.log_metrics(metrics)
        mlflow.log_param("artifact_path", artifact_uri)
        if s3_uri:
            mlflow.log_param("s3_uri", s3_uri)
        return run.info.run_id


def upload_run_to_s3(run_dir: Path, run_id: str) -> None:
    """Upload run directory to S3/MinIO, with retry on transient failures."""
    bucket = os.environ["S3_BUCKET"]
    endpoint_url = os.environ["S3_ENDPOINT_URL"]
    
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    
    files = [f for f in run_dir.rglob("*") if f.is_file()]    
    for file_path in files:
        s3_key = f"{run_id}/{file_path.relative_to(run_dir)}"
        attempt = 0
        delay = 5
        while True:
            try:
                s3.upload_file(str(file_path), bucket, s3_key)
                break
            except Exception as e:
                attempt += 1
                if attempt == 3:
                    raise
                print(f"Upload failed for {s3_key} (attempt {attempt}/3): {e}. Retrying in {delay}s...")
                time.sleep(delay)
                delay *= 2
    
    return None
