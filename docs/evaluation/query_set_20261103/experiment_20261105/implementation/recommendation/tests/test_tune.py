import contextlib
import io
import tempfile
import unittest
from pathlib import Path

import numpy as np

from recommendation.evaluate import evaluate_engine
from recommendation.tests.test_recommendation import NOW, fixture
from recommendation.train import train
from recommendation.tune import (SearchEngine, bootstrap_delta, query_metrics,
                                 select_weights, summarize, weight_candidates)


class TuningTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        root = Path(cls.temp.name)
        source = root/'data.parquet'
        fixture().to_parquet(source, index=False)
        cls.bundle = root/'bundle'
        with contextlib.redirect_stdout(io.StringIO()):
            train(source, cls.bundle, dataset_version='test-v1', as_of=NOW, min_df=1)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def setUp(self):
        self.engine = SearchEngine(self.bundle)

    def task(self, method='similar', split='tune', **request):
        args = dict(listing_key='rent:0') if method == 'similar' else dict(
            transaction_type='rent', property_category='HOUSE', price_period='month',
            query='gần trường học', target_price=10e6, target_area=80)
        args.update(request)
        return dict(query_id='q1', dataset_version='test-v1', split=split, as_of=NOW,
                    method=method, request=args,
                    relevance={f'rent:{i}': grade for i, grade in enumerate([3, 2, 1, 0, 0, 0])})

    def test_cached_full_catalog_ranking_matches_serving(self):
        for method in ('similar', 'preferences'):
            task = self.task(method)
            cache = self.engine.capture(task)
            baseline = dict(self.engine.manifest['weights']['rent'])
            for weights in weight_candidates(baseline, random_count=12):
                self.engine.manifest['weights']['rent'] = weights
                call = self.engine.similar if method == 'similar' else self.engine.preferences
                expected = [row['listing_key'] for row in call(**task['request'], as_of=NOW)]
                self.assertEqual(cache.rank(weights), expected)
            self.assertEqual(len(cache.keys), len(set(cache.keys)))
            self.engine.manifest['weights']['rent'] = baseline

    def test_stable_ties_and_duplicate_groups(self):
        task = self.task('preferences', query=None, target_price=None, target_area=None)
        catalog = self.engine.markets['rent'][0]
        catalog.loc[catalog.listing_key.eq('rent:1'), 'duplicate_group'] = 'rent:0'
        cache = self.engine.capture(task)
        keys = cache.rank(self.engine.manifest['weights']['rent'])
        self.assertEqual(keys[0], 'rent:0')
        self.assertNotIn('rent:1', keys)
        self.assertEqual(keys, cache.baseline_keys)

    def test_no_text_match_yields_empty_metrics(self):
        cache = self.engine.capture(self.task('preferences', query='zzzzqqqxxx'))
        keys = cache.rank(self.engine.manifest['weights']['rent'])
        self.assertEqual(keys, [])
        metrics = query_metrics(cache, keys)
        self.assertEqual(metrics['precision@10'], 0.)
        self.assertEqual(metrics['ndcg@10'], 0.)
        self.assertTrue(metrics['short_result'])

    def test_metrics_equal_official_evaluator_and_short_pool_denominator(self):
        task = self.task('preferences')
        cache = self.engine.capture(task)
        rows = [query_metrics(cache, cache.baseline_keys)]
        report = summarize(rows)['rent']
        official = evaluate_engine(self.engine, [task])['rent/tune']
        for metric in ('ndcg@10', 'precision@10', 'pooled_recall@10'):
            self.assertEqual(report[metric], official[metric])
        hits = sum(task['relevance'][key] > 0 for key in cache.baseline_keys)
        self.assertEqual(report['precision@10'], hits/10)

    def test_unjudged_topk_never_becomes_zero(self):
        cache = self.engine.capture(self.task())
        del cache.task['relevance'][cache.baseline_keys[0]]
        with self.assertRaisesRegex(ValueError, 'Unjudged Top-K'):
            query_metrics(cache, cache.baseline_keys)

    def test_test_split_rejected_by_selection(self):
        cache = self.engine.capture(self.task(split='test'))
        weights = self.engine.manifest['weights']['rent']
        with self.assertRaisesRegex(ValueError, 'only nonempty tune'):
            select_weights([cache], [weights], weights)

    def test_search_is_reproducible_and_baseline_wins_tie(self):
        weights = self.engine.manifest['weights']['rent']
        candidates = weight_candidates(weights, random_count=8)
        self.assertEqual(candidates, weight_candidates(weights, random_count=8))
        cache = self.engine.capture(self.task('preferences', query=None, target_price=None, target_area=None))
        best, _ = select_weights([cache], candidates, weights)
        self.assertEqual(best['candidate_id'], 0)
        invalid = dict(weights, text=0.)
        with self.assertRaises(ValueError):
            cache.rank(invalid)
        with self.assertRaises(ValueError):
            cache.rank(weights, 0)

    def test_paired_bootstrap_and_alignment(self):
        before = [dict(query_id='q1', **{'ndcg@10': .5})]
        after = [dict(query_id='q1', **{'ndcg@10': .7})]
        result = bootstrap_delta(before, after, samples=100)
        np.testing.assert_allclose(result['percentile_95_interval'], [.2, .2])
        with self.assertRaisesRegex(ValueError, 'paired queries'):
            bootstrap_delta(before, [dict(query_id='q2', **{'ndcg@10': .7})])


if __name__ == '__main__':
    unittest.main()
