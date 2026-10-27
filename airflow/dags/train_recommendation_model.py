"""Retrain only after Gold tests succeed; isolated worker owns the ML environment."""
import time
from datetime import timedelta

import pendulum
import requests
from airflow import DAG
from airflow.datasets import Dataset
from airflow.models import Variable
from airflow.operators.python import PythonOperator


def retrain(run_id, **context):
    endpoint = 'http://recommendation-worker:8010/jobs'
    headers = {'X-Admin-Token': Variable.get('recommendation_admin_token')}
    job_id = 'airflow:' + run_id
    response = requests.post(endpoint, json={'job_id': job_id}, headers=headers, timeout=30)
    response.raise_for_status()
    deadline = time.monotonic() + 3600
    while time.monotonic() < deadline:
        response = requests.get(endpoint, params={'job_id': job_id}, headers=headers, timeout=30)
        response.raise_for_status()
        state = response.json()
        if state['status'] == 'succeeded':
            print('Recommendation deployed, Registry version:', state['version'])
            return state
        if state['status'] == 'failed':
            raise RuntimeError('Recommendation retrain failed; inspect worker.log in the job directory.')
        time.sleep(10)
    raise TimeoutError('Recommendation worker exceeded the 1-hour polling limit; inspect job before retry.')


with DAG(
    dag_id='train_recommendation_model',
    description='Gold validation → export → train → mechanical gates → Registry → FastAPI reload',
    schedule=[Dataset('lakehouse://gold/ml_recommendation_features')],
    start_date=pendulum.datetime(2026, 10, 1, tz='Asia/Ho_Chi_Minh'),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    tags=['recommendation', 'mlflow', 'gold'],
) as dag:
    PythonOperator(
        task_id='retrain_register_deploy',
        python_callable=retrain,
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=65),
    )
