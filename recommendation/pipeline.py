"""Retry-safe export -> train -> validate -> register -> deploy pipeline."""
import argparse
import fcntl
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import requests
from mlflow.tracking import MlflowClient

from .contract import utc
from .registry import register, MODEL_NAME


def write_json(path, value):
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2))
    os.replace(temp, path)


def run(job_id, bootstrap=None):
    root = Path(os.getenv('REIP_STATE', '/state'))
    root.mkdir(parents=True, exist_ok=True)
    with (root/'pipeline.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        directory = root/'jobs'/hashlib.sha256(job_id.encode()).hexdigest()[:24]
        directory.mkdir(parents=True, exist_ok=True)
        plan_path = directory/'plan.json'
        if plan_path.exists():
            plan = json.loads(plan_path.read_text())
            if plan.get('status') == 'deployed':
                print(json.dumps(plan, ensure_ascii=False), flush=True)
                return plan
        else:
            plan = dict(job_id=job_id, as_of=utc().isoformat())
            write_json(plan_path, plan)
        snapshot = None
        if bootstrap:
            bundle = Path(bootstrap)
        else:
            exported = directory/'export'
            if not exported.exists():
                print('Exporting a consistent Gold Delta snapshot', flush=True)
                subprocess.run([sys.executable, '-m', 'recommendation.export', '--endpoint',
                    os.getenv('MLFLOW_S3_ENDPOINT_URL', 'http://minio:9000'), '--output', str(exported)], check=True)
            snapshot = json.loads((exported/'snapshot.json').read_text())
            bundle = directory/'bundle'
            dataset_version = f"gold.ml_recommendation_features.{snapshot['delta_table_id']}.delta_v{snapshot['delta_version']}"
            if not bundle.exists():
                print('Training separate sale and rent indices', flush=True)
                subprocess.run([sys.executable, '-m', 'recommendation.train', '--input', str(exported/'catalog.parquet'),
                    '--output', str(bundle), '--dataset-version', dataset_version, '--as-of', plan['as_of']], check=True)
        report_file = directory/'mechanical_report.json'
        if not report_file.exists():
            print('Running mechanical validation', flush=True)
            subprocess.run([sys.executable, '-m', 'recommendation.benchmark', '--bundle', str(bundle),
                            '--queries', '15', '--output', str(report_file)], check=True)
        report = json.loads(report_file.read_text())
        for market in ('sale', 'rent'):
            stats = report['markets'][market]
            if stats['rows'] < 1 or stats['similar_violations'] or stats['preference_violations'] or not stats['similar_results']:
                raise ValueError(f'Mechanical checks failed for {market}')
        if 'registered' not in plan:
            print('Logging artifacts and registering model version', flush=True)
            plan['registered'] = register(bundle, report, snapshot, directory.name)
            write_json(plan_path, plan)
        registered = plan['registered']
        client = MlflowClient()
        if 'previous_champion' not in plan:
            plan['previous_champion'] = client.get_registered_model(MODEL_NAME).aliases.get('champion')
            write_json(plan_path, plan)
        previous = plan['previous_champion']
        version = registered['version']
        client.set_registered_model_alias(MODEL_NAME, 'champion', version)
        try:
            token = Path(os.environ['REIP_ADMIN_TOKEN_FILE']).read_text().strip()
            response = requests.post(os.getenv('REIP_API_URL', 'http://recommendation-api:8000')+'/admin/reload',
                headers={'X-Admin-Token': token}, json={'version': version}, timeout=240)
            response.raise_for_status()
            if response.json()['version'] != version:
                raise ValueError('API loaded a different version')
        except Exception:
            # Compare before rollback so a separate manual promotion is not overwritten.
            if client.get_model_version_by_alias(MODEL_NAME, 'champion').version == version:
                if previous:
                    client.set_registered_model_alias(MODEL_NAME, 'champion', previous)
                else:
                    client.delete_registered_model_alias(MODEL_NAME, 'champion')
            raise
        plan.update(status='deployed', deployed_version=version, completed_at=utc().isoformat(),
                    promotion_policy='mechanical_checks_only_demo')
        write_json(plan_path, plan)
        print(json.dumps(plan, ensure_ascii=False), flush=True)
        return plan


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--job-id', required=True)
    parser.add_argument('--bootstrap', help='Register an existing bundle instead of retraining')
    args = parser.parse_args()
    run(args.job_id, args.bootstrap)
