"""Log a complete immutable bundle, then register it as a real MLflow Model."""
import json
import shutil
import tempfile
from pathlib import Path
import importlib.metadata

import mlflow
from mlflow.models import ModelSignature
from mlflow.tracking import MlflowClient
from mlflow.types.schema import Schema, ColSpec

from .contract import file_hash
from .mlflow_model import RecommendationModel

MODEL_NAME = 'reip_recommendation'


def register(bundle, report, snapshot=None, job_id='manual'):
    bundle = Path(bundle)
    manifest = json.loads((bundle/'manifest.json').read_text())
    mlflow.set_experiment(MODEL_NAME)
    client = MlflowClient()
    fingerprint = file_hash(bundle/'manifest.json')
    # Recover from a crash after registration without creating a duplicate version.
    for version in client.search_model_versions(f"name='{MODEL_NAME}'"):
        if version.tags.get('bundle_sha256') == fingerprint:
            return {'version': str(version.version), 'run_id': version.run_id, 'fingerprint': fingerprint}
    with mlflow.start_run(run_name=f'recommendation-{job_id}') as run:
        mlflow.set_tags({'dataset_version': manifest['dataset_version'], 'bundle_sha256': fingerprint,
            'job_id': job_id, 'quality_status': 'not_evaluated_with_human_labels',
            'promotion_policy': 'mechanical_checks_only_demo', 'as_of': manifest['as_of']})
        mlflow.log_params({k: manifest[k] for k in ['max_age_days', 'max_features', 'min_df', 'sklearn']})
        mlflow.log_params({f'{market}_{key}_weight': value for market, weights in manifest['weights'].items() for key, value in weights.items()})
        for market, stats in report['markets'].items():
            mlflow.log_metrics({f'{market}_{key}': value for key, value in stats.items() if type(value) in (int, float)})
        mlflow.log_dict(manifest, 'manifest.json')
        mlflow.log_dict(report, 'mechanical_report.json')
        if snapshot:
            mlflow.log_dict(snapshot, 'snapshot.json')
        with tempfile.TemporaryDirectory() as temp:
            package = Path(temp)/'recommendation'
            package.mkdir()
            for name in ['__init__.py', 'contract.py', 'engine.py', 'mlflow_model.py']:
                shutil.copy2(Path(__file__).parent/name, package/name)
            mlflow.log_dict({p.name: file_hash(p) for p in package.iterdir()}, 'code_checksums.json')
            signature = ModelSignature(inputs=Schema([ColSpec('string', 'method'), ColSpec('string', 'request_json')]),
                                       outputs=Schema([ColSpec('string', 'result_json')]))
            info = mlflow.pyfunc.log_model(name='recommendation', python_model=RecommendationModel(),
                artifacts={'bundle': str(bundle)}, code_paths=[str(package)], signature=signature,
                pip_requirements=[f'{p}=={importlib.metadata.version(p)}' for p in
                    ['mlflow', 'numpy', 'pandas', 'scipy', 'scikit-learn', 'pyarrow', 'joblib', 'cloudpickle']])
        version = mlflow.register_model(info.model_uri, MODEL_NAME, tags={'bundle_sha256': fingerprint,
            'dataset_version': manifest['dataset_version'], 'promotion_policy': 'mechanical_checks_only_demo'})
        client.set_registered_model_alias(MODEL_NAME, 'candidate', version.version)
        return {'version': str(version.version), 'run_id': run.info.run_id, 'fingerprint': fingerprint}
