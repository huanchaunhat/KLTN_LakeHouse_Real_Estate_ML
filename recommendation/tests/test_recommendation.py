import contextlib
import io
import json
import shutil
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.parquet as pq

from recommendation.contract import file_hash
from recommendation.engine import RecommendationEngine, location_similarity
from recommendation.evaluate import evaluate
from recommendation.export import read_snapshot
from recommendation.train import train

NOW = '2026-10-23T00:00:00Z'


def fixture():
    rows = []
    for market, price, period in [('sale', 3e9, 'total'), ('rent', 10e6, 'month')]:
        for i in range(6):
            rows.append(dict(
                listing_key=f'{market}:{i}', transaction_type=market, property_category='HOUSE',
                price_period=period, price_vnd=price*(1+.02*i), area_m2=80+i,
                bedrooms=2, bathrooms=1, last_seen_at='2026-10-22T00:00:00Z',
                is_recommendable=True, location_trusted=True,
                content_text='Nhà gần trường học hẻm xe hơi đầy đủ nội thất',
                title=f'Nhà {i}', province_id='1', district_id='10', ward_id='100',
                latitude=10.0, longitude=105.0, duplicate_group=f'{market}:{i}'))
    rows[2]['duplicate_group'] = 'sale:1'
    rows[3]['province_id'] = '2'
    rows[4]['is_recommendable'] = False
    rows[5]['last_seen_at'] = '2027-01-01T00:00:00Z'
    return pd.DataFrame(rows)


class RecommendationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temp.name)
        cls.source = cls.root / 'data.parquet'
        fixture().to_parquet(cls.source, index=False)
        cls.bundle = cls.root / 'bundle'
        with contextlib.redirect_stdout(io.StringIO()):
            train(cls.source, cls.bundle, dataset_version='test-v1', as_of=NOW, min_df=1)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def setUp(self):
        self.engine = RecommendationEngine(self.bundle)

    def test_market_locality_self_and_duplicate_constraints(self):
        hits = self.engine.similar('sale:0', as_of=NOW)
        self.assertEqual([r['listing_key'] for r in hits], ['sale:1'])
        self.assertTrue(all(r['transaction_type'] == 'sale' for r in hits))
        self.assertTrue(all(r['transaction_type'] == 'rent' for r in self.engine.similar('rent:0', as_of=NOW)))

    def test_known_removed_and_future_not_in_model(self):
        self.assertNotIn('sale:4', self.engine.lookup)
        self.assertNotIn('sale:5', self.engine.lookup)

    def test_serving_rechecks_age(self):
        self.assertEqual(self.engine.similar('sale:0', as_of='2027-10-23T00:00:00Z'), [])

    def test_serving_rechecks_future_date_and_rent_period(self):
        df = self.engine.markets['rent'][0]
        df.loc[df.listing_key.eq('rent:1'), 'price_period'] = 'day'
        df.loc[df.listing_key.eq('rent:2'), 'last_seen_at'] = pd.Timestamp('2027-01-01T00:00:00Z')
        keys = {r['listing_key'] for r in self.engine.similar('rent:0', as_of=NOW)}
        self.assertNotIn('rent:1', keys)
        self.assertNotIn('rent:2', keys)

    def test_rent_requires_explicit_period_and_budget_is_hard(self):
        with self.assertRaises(ValueError):
            self.engine.preferences(transaction_type='rent', property_category='HOUSE', max_price=11e6, as_of=NOW)
        hits = self.engine.preferences(transaction_type='rent', property_category='HOUSE',
                                       price_period='month', max_price=10.3e6,
                                       min_area=81, min_bedrooms=2, as_of=NOW)
        self.assertEqual([r['listing_key'] for r in hits], ['rent:1'])

    def test_unknown_rent_period_cannot_use_numeric_budget(self):
        with self.assertRaises(ValueError):
            self.engine.preferences(transaction_type='rent', property_category='HOUSE',
                                    price_period='unknown', max_price=10e6, as_of=NOW)

    def test_vietnamese_query_normalization_and_no_match(self):
        args = dict(transaction_type='sale', property_category='HOUSE', as_of=NOW)
        a = self.engine.preferences(query='gần trường học', **args)
        b = self.engine.preferences(query='gan truong hoc', **args)
        self.assertEqual(a, b)
        self.assertEqual(self.engine.preferences(query='zzzzqqqxxx', **args), [])

    def test_location_does_not_match_district_without_province(self):
        frame = self.engine.markets['sale'][0].iloc[[0]].copy()
        seed = frame.iloc[0].copy()
        seed['latitude'] = seed['longitude'] = np.nan
        frame['province_id'] = '2'
        self.assertEqual(location_similarity(frame, seed).tolist(), [0.])

    def test_tampered_catalog_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / 'bundle'
            shutil.copytree(self.bundle, target)
            file = target / 'rent/catalog.parquet'
            pd.read_parquet(file).iloc[::-1].to_parquet(file, index=False)
            with self.assertRaisesRegex(ValueError, 'checksum'):
                RecommendationEngine(target)
            manifest = json.loads((target/'manifest.json').read_text())
            manifest['markets']['rent']['files']['catalog.parquet'] = file_hash(file)
            (target/'manifest.json').write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, 'alignment'):
                RecommendationEngine(target)

    def test_duplicate_input_and_immutable_output_rejected(self):
        with self.assertRaises(FileExistsError):
            train(self.source, self.bundle, dataset_version='new', as_of=NOW)
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp)/'bad.parquet'
            pd.concat([fixture(), fixture().iloc[[0]]]).to_parquet(source, index=False)
            with self.assertRaisesRegex(ValueError, 'Duplicate'):
                train(source, Path(tmp)/'out', dataset_version='new', as_of=NOW)

    def test_evaluation_uses_fixed_time_age_and_market(self):
        task = dict(dataset_version='test-v1', split='test', as_of=NOW,
                    method='preferences', request=dict(transaction_type='rent',
                    property_category='HOUSE', price_period='month', max_price=10e6,
                    max_age_days=90), relevance={'rent:0': 3})
        with tempfile.TemporaryDirectory() as tmp:
            labels = Path(tmp)/'labels.jsonl'
            labels.write_text(json.dumps(task))
            report = evaluate(self.bundle, labels, k=1)
            self.assertEqual(report['rent/test']['ndcg@1'], 1.)
            with self.assertRaises(ValueError):
                evaluate(self.bundle, labels, k=0)
            task['dataset_version'] = 'wrong'
            labels.write_text(json.dumps(task))
            with self.assertRaises(ValueError):
                evaluate(self.bundle, labels, k=1)

    def test_delta_reader_replays_removals_and_partition_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root/'_delta_log').mkdir()
            pq.write_table(pa.table({'id': ['old']}), root/'old.parquet')
            pq.write_table(pa.table({'id': ['new']}), root/'new.parquet')
            actions = [dict(protocol=dict(minReaderVersion=1)),
                       dict(metaData=dict(id='test', configuration={})),
                       dict(add=dict(path='old.parquet', partitionValues={'transaction_type': 'sale'}))]
            (root/'_delta_log/00000000000000000000.json').write_text('\n'.join(map(json.dumps, actions)))
            actions = [dict(remove=dict(path='old.parquet')),
                       dict(add=dict(path='new.parquet', partitionValues={'transaction_type': 'rent'}))]
            (root/'_delta_log/00000000000000000001.json').write_text('\n'.join(map(json.dumps, actions)))
            table, meta = read_snapshot(fs.LocalFileSystem(), str(root))
            self.assertEqual(table.to_pylist(), [dict(id='new', transaction_type='rent')])
            self.assertEqual(meta['delta_version'], 1)


if __name__ == '__main__':
    unittest.main()
