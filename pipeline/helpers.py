import json
import os
import shutil
import subprocess
import boto3
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parents[1]

def build_run_config(params: dict, current_run_id: str) -> dict:
    """Build a run configuration dict from Airflow params and current_run_id."""
    return {
        "run_id": current_run_id,  # Keep the JSON payload key as 'run_id' for your scripts
        "split": params["split"],
        "subset": params["subset"],
        "workers": params["workers"],
        "model": params["model"],
        "task_slice": params["task_slice"],
        "cost_limit": params["cost_limit"],
        "created_at": datetime.utcnow().isoformat(),
    }


def prepare_run_dir(run_dir: Path, run_config: dict) -> Path:
    """Create the run directory structure and write config.json."""
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "run-agent").mkdir(exist_ok=True)
    (run_dir / "run-eval" / "logs").mkdir(parents=True, exist_ok=True)
    (run_dir / "run-eval" / "reports").mkdir(parents=True, exist_ok=True)
    with open(run_dir / "config.json", "w") as f:
        json.dump(run_config, f, indent=2)
    return run_dir


def fix_preds_location(run_dir: Path) -> Path:
    """Move preds.json from trajectories/ up to run-agent/ if misplaced."""
    agent_out_dir = run_dir / "run-agent"
    misplaced = agent_out_dir / "trajectories" / "preds.json"
    expected = agent_out_dir / "preds.json"
    if misplaced.exists():
        shutil.move(str(misplaced), str(expected))
    return expected


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


def run_agent_batch(run_config: dict, run_dir: Path) -> Path:
    """Run mini-swe-agent batch script and return path to preds.json. Used by evaluate_agent_local DAG only."""
    agent_out_dir = run_dir / "run-agent" 
    subprocess.run(
        [
            "bash",
            str(PROJECT_ROOT / "scripts" / "mini-swe-bench-batch.sh"),
            run_config["split"],
            run_config["subset"],
            str(run_config["workers"]),
            run_config["model"],
            run_config["task_slice"],
            str(agent_out_dir / "trajectories"),
        ],
        cwd=PROJECT_ROOT,
        env={**os.environ, "MSWEA_COST_TRACKING": "ignore_errors"},
        check=True,
    )
    # Post-execution fix: Pull preds.json out of trajectories up into run-agent
    expected_preds = fix_preds_location(run_dir)        
    return expected_preds


def run_swebench_eval(run_config: dict, preds_path: Path, run_dir: Path) -> Path:
    """Run SWE-bench evaluation on predictions and return eval output dir. Used by evaluate_agent_local DAG only."""
    eval_out_dir = run_dir / "run-eval"
    subprocess.run(
        [
            "bash",
            str(PROJECT_ROOT / "scripts" / "swe-bench-eval.sh"),
            str(preds_path),
            str(run_config["workers"]),
            run_config["run_id"],
            str(eval_out_dir / "reports"),        # bug in make_run_report() in reporting.py: writes the report to cwd instead of report_dir.
        ],
        cwd=str(eval_out_dir),
        check=True,
    )
    # Post-execution fix: move the generated report into the reports/ subfolder
    fix_report_location(run_dir, run_config['run_id'])
    return eval_out_dir


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
        "total_instances": report.get("total_instances", 0),
        "resolve_rate": report.get("resolved_instances", 0) / max(report.get("submitted_instances", 1), 1),
    }


def build_manifest(run_id: str, run_dir: Path, s3_uri: str = None) -> dict:
    """Build manifest dictionary pointing to all run artifacts."""
    return {
        "run_id": run_id,
        "artifact_path": str(run_dir),
        "config": str(run_dir / "config.json"),
        "metrics": str(run_dir / "metrics.json"),
        "predictions": str(run_dir / "run-agent" / "preds.json"),
        "trajectories": str(run_dir / "run-agent" / "trajectories"),
        "logs": str(run_dir / "run-eval" / "logs"),
        "reports": str(run_dir / "run-eval" / "reports"),
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
    """Upload run directory to S3/MinIO."""
    bucket = os.environ["S3_BUCKET"]
    endpoint_url = os.environ["S3_ENDPOINT_URL"]
    
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    
    for file_path in run_dir.rglob("*"):
        if file_path.is_file():
            s3_key = f"{run_id}/{file_path.relative_to(run_dir)}"
            s3.upload_file(str(file_path), bucket, s3_key)
    
    return None


