"""HTTP/annotation/rollback regression checks using temporary synthetic data only."""
import contextlib
import io
import hashlib
import json
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

import mlflow.pyfunc
import pandas as pd
import requests
import uvicorn

from recommendation.contract import file_hash
from api.app.recommendation.main import Manager, create_app
from recommendation.engine import RecommendationEngine
from recommendation.evaluate import evaluate
from recommendation.mlflow_model import RecommendationModel
from recommendation.tests.test_recommendation import fixture, NOW
from recommendation.train import train
from recommendation.pipeline import run


class DeploymentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temp.name)
        source = cls.root/'data.parquet'
        fixture().to_parquet(source, index=False)
        cls.bundle = cls.root/'bundle'
        with contextlib.redirect_stdout(io.StringIO()):
            train(source, cls.bundle, dataset_version='http-test', as_of=NOW, min_df=1)
        cls.manager = Manager()
        cls.manager.state = dict(engine=RecommendationEngine(cls.bundle), version='1',
                                fingerprint=file_hash(cls.bundle/'manifest.json'))
        cls.manager.load = lambda version=None: cls.manager.info()
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            cls.port = sock.getsockname()[1]
        cls.server = uvicorn.Server(uvicorn.Config(create_app(cls.manager, cls.root/'labels.sqlite'),
            host='127.0.0.1', port=cls.port, log_level='error'))
        cls.thread = threading.Thread(target=cls.server.run, daemon=True)
        cls.thread.start()
        for _ in range(100):
            if cls.server.started:
                break
            time.sleep(.05)
        if not cls.server.started:
            raise RuntimeError('Test HTTP server did not start')
        cls.url = f'http://127.0.0.1:{cls.port}'

    @classmethod
    def tearDownClass(cls):
        cls.server.should_exit = True
        cls.thread.join(10)
        cls.temp.cleanup()

    def task(self):
        r = requests.post(self.url+'/tasks', json=dict(method='similar',
            request=dict(listing_key='rent:0', limit=5, as_of=NOW),
            model_version='1', annotator='synthetic-test-only', split='test', k=2))
        self.assertEqual(r.status_code, 201, r.text)
        return r.json()

    def test_market_and_validation(self):
        for market in ('sale', 'rent'):
            r = requests.post(self.url+'/recommend', json=dict(method='similar',
                request=dict(listing_key=market+':0', as_of=NOW)))
            self.assertEqual(r.status_code, 200, r.text)
            self.assertTrue(r.json()['items'])
            self.assertTrue(all(x['transaction_type'] == market for x in r.json()['items']))
        for body in [dict(method='similar', request={'listing_key':'rent:0','limit':True}),
                     dict(method='preferences', request={'transaction_type':'rent','property_category':'HOUSE'}),
                     dict(method='similar', request={'listing_key':'rent:0','as_of':'invalid'})]:
            self.assertEqual(requests.post(self.url+'/recommend', json=body).status_code, 422)

    def test_frozen_labels_zero_missing_conflict_export_and_metrics(self):
        task = self.task()
        url = self.url+'/tasks/'+task['query_id']
        export = self.url+'/export?task_id='+task['query_id']
        self.assertEqual(requests.get(export).status_code, 409)
        key = next(iter(task['relevance']))
        for invalid in [True, -1, 4, 1.5]:
            self.assertEqual(requests.patch(url,json=dict(listing_key=key,grade=invalid,version=task['version'])).status_code,422)
        payload = dict(listing_key=key, grade=0, version=task['version'])
        r = requests.patch(url, json=payload)
        self.assertEqual(r.status_code, 200)
        task = r.json()
        self.assertIs(task['relevance'][key], 0)
        self.assertEqual(requests.patch(url,json=payload).status_code,409)
        self.assertEqual(requests.get(export).status_code,409)
        for key in task['relevance']:
            task = requests.patch(url, json=dict(listing_key=key, grade=3,
                         note='=SUM(1)', version=task['version'])).json()
        r = requests.get(export)
        self.assertEqual(r.status_code,200)
        labels = self.root/'labels.jsonl'
        labels.write_text(r.text)
        self.assertNotIn('items', json.loads(r.text))
        expected = evaluate(self.bundle, labels, k=2)
        actual = requests.get(url+'/metrics')
        self.assertEqual(actual.status_code,200,actual.text)
        self.assertEqual(actual.json()['metrics'],expected)
        self.assertIn("'=SUM(1)", requests.get(export+'&format=csv').text)
        previous = self.manager.state
        try:
            self.manager.state = {**previous, 'version':'2', 'fingerprint':'new-fingerprint'}
            self.assertEqual(requests.get(url).json()['items'], task['items'])
            self.assertEqual(requests.get(url+'/metrics').status_code,409)
            self.assertEqual(requests.get(export).status_code,200)
            self.assertEqual(requests.post(self.url+'/tasks',json=dict(method='similar',request={'listing_key':'rent:0'},
                model_version='1',annotator='x')).status_code,409)
        finally:
            self.manager.state = previous

    def test_failed_reload_keeps_previous_engine(self):
        manager = Manager()
        manager.state = self.manager.state
        old = manager.state
        with patch('api.app.recommendation.main.ROOT', self.root), \
             patch('api.app.recommendation.main.MlflowClient'), \
             patch('mlflow.pyfunc.load_model', side_effect=RuntimeError('corrupt candidate')):
            with self.assertRaisesRegex(RuntimeError, 'corrupt candidate'):
                manager.load('2')
        self.assertIs(manager.state, old)

    def test_pyfunc_roundtrip_predict(self):
        path = self.root/'pyfunc'
        mlflow.pyfunc.save_model(path=str(path), python_model=RecommendationModel(),
            artifacts={'bundle':str(self.bundle)}, pip_requirements=[])
        model = mlflow.pyfunc.load_model(str(path))
        out = model.predict(pd.DataFrame([dict(method='similar',request_json=json.dumps(
            dict(listing_key='rent:0',limit=2,as_of=NOW)))]))
        expected = self.manager.state['engine'].similar('rent:0', limit=2, as_of=NOW)
        self.assertEqual(json.loads(out.iloc[0].result_json), expected)

    def test_pipeline_rolls_back_alias_and_completed_retry_is_noop(self):
        job_id = 'rollback-test'
        root = self.root/'pipeline'
        directory = root/'jobs'/hashlib.sha256(job_id.encode()).hexdigest()[:24]
        directory.mkdir(parents=True)
        (root/'token').write_text('synthetic-test-token')
        report = {'markets': {m: dict(rows=6,similar_violations=0,preference_violations=0,similar_results=5)
                              for m in ('sale','rent')}}
        (directory/'mechanical_report.json').write_text(json.dumps(report))
        client = MagicMock()
        client.get_registered_model.return_value.aliases = {'champion':'1'}
        client.get_model_version_by_alias.return_value.version = '2'
        with patch.dict('os.environ', {'REIP_STATE':str(root), 'REIP_ADMIN_TOKEN_FILE':str(root/'token')}), \
             patch('recommendation.pipeline.MlflowClient', return_value=client), \
             patch('recommendation.pipeline.register',return_value={'version':'2'}) as register, \
             patch('recommendation.pipeline.requests.post', side_effect=requests.ConnectionError('offline')):
            with self.assertRaises(requests.ConnectionError):
                run(job_id, self.bundle)
            calls = [call.args for call in client.set_registered_model_alias.call_args_list]
            self.assertEqual([call[2] for call in calls], ['2','1'])
            plan_path = directory/'plan.json'
            plan = json.loads(plan_path.read_text())
            self.assertEqual(plan['previous_champion'],'1')
            plan['status'] = 'deployed'
            plan_path.write_text(json.dumps(plan))
            client.reset_mock()
            register.reset_mock()
            with contextlib.redirect_stdout(io.StringIO()):
                run(job_id,self.bundle)
            client.set_registered_model_alias.assert_not_called()
            register.assert_not_called()


if __name__ == '__main__':
    unittest.main()
