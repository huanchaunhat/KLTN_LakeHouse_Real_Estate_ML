"""Reproducible weight search over the serving engine's full candidate lists.

Labels are supplied separately. Unknown Top-K judgments always raise; this
module never substitutes zero or restricts retrieval to the judged pool.
"""
from dataclasses import dataclass
import math

import numpy as np

from .contract import DEFAULT_WEIGHTS
from .engine import RecommendationEngine


WEIGHT_NAMES = tuple(DEFAULT_WEIGHTS)


@dataclass
class CachedRanking:
    task: dict
    market: str
    keys: np.ndarray
    groups: np.ndarray
    parts: dict
    exclude_group: str | None
    baseline_keys: list

    def rank(self, weights, k=10):
        if set(weights) != set(WEIGHT_NAMES) or any(
                not np.isfinite(v) or v <= 0 for v in weights.values()):
            raise ValueError('All six weights must be finite and positive')
        if type(k) is not int or not 1 <= k <= 50:
            raise ValueError('k must be an integer in [1, 50]')
        if not self.parts:
            return []
        # Retain the serving engine's summation order, dtypes and stable ties.
        denominator = sum(weights[name] for name in self.parts)
        scores = sum(weights[name] * values for name, values in self.parts.items()) / denominator
        order = np.argsort(-scores, kind='stable')
        used = {self.exclude_group} if self.exclude_group else set()
        result = []
        for index in order:
            group = self.groups[index]
            if group in used:
                continue
            used.add(group)
            result.append(self.keys[index])
            if len(result) == k:
                break
        return result


class SearchEngine(RecommendationEngine):
    """Capture score components by executing the real serving implementation."""

    def _rank(self, market, ids, parts, as_of, limit, exclude_group=None):
        output = super()._rank(market, ids, parts, as_of, limit, exclude_group)
        catalog = self.markets[market][0].iloc[ids]
        self._captured = (catalog.listing_key.to_numpy(), catalog.duplicate_group.to_numpy(),
                          dict(parts), exclude_group)
        return output

    def capture(self, task, k=10):
        if task['dataset_version'] != self.manifest['dataset_version']:
            raise ValueError('Annotation catalog version does not match the bundle')
        if task['split'] not in ('tune', 'test'):
            raise ValueError('Unknown annotation split')
        args = dict(task['request'], as_of=task['as_of'], limit=k)
        self._captured = None
        if task['method'] == 'similar':
            market = self.lookup[args['listing_key']][0]
            output = self.similar(**args)
        elif task['method'] == 'preferences':
            market = args['transaction_type']
            output = self.preferences(**args)
        else:
            raise ValueError('Unknown evaluation method')
        captured = self._captured or (np.array([], dtype=object), np.array([], dtype=object), {}, None)
        cache = CachedRanking(task, market, *captured, [r['listing_key'] for r in output])
        if cache.rank(self.manifest['weights'][market], k) != cache.baseline_keys:
            raise ValueError('Cached ranking differs from serving')
        return cache


def weight_candidates(baseline, seed=20261105, random_count=256):
    """Declare a finite search before reading any tuning metrics."""
    if random_count < 0:
        raise ValueError('random_count must be nonnegative')
    candidates = [dict(baseline),
                  dict(text=.8, price=.04, area=.04, location=.04, structure=.04, freshness=.04),
                  dict(text=.01, price=.4, area=.25, location=.2, structure=.1, freshness=.04)]
    for name in WEIGHT_NAMES:
        for scale in (.5, 2.):
            candidate = dict(baseline)
            candidate[name] *= scale
            candidates.append(candidate)
    rng = np.random.default_rng(seed)
    for _ in range(random_count):
        values = .01 + .94 * rng.dirichlet([2, 2, 2, 2, 1, 1])
        candidates.append(dict(zip(WEIGHT_NAMES, map(float, values))))
    return candidates


def query_metrics(cache, keys, k=10):
    relevance = cache.task['relevance']
    if not relevance or any(type(v) is not int or not 0 <= v <= 3 for v in relevance.values()):
        raise ValueError('Judgments must contain integer grades 0..3')
    missing = set(keys) - set(relevance)
    if missing:
        raise ValueError(f'Unjudged Top-K for {cache.task["query_id"]}: {sorted(missing)}')
    gains = [2**relevance[key]-1 for key in keys]
    dcg = sum(gain / math.log2(i+2) for i, gain in enumerate(gains))
    ideal = sum((2**grade-1) / math.log2(i+2)
                for i, grade in enumerate(sorted(relevance.values(), reverse=True)[:k]))
    positives = sum(grade > 0 for grade in relevance.values())
    hits = sum(relevance[key] > 0 for key in keys)
    return dict(query_id=cache.task['query_id'], market=cache.market,
                split=cache.task['split'], method=cache.task['method'],
                judged_pool_size=len(relevance), candidate_count=len(cache.keys),
                returned=len(keys), short_result=len(keys) < k,
                **{f'precision@{k}': hits/k,
                   f'precision_grade_ge_2@{k}': sum(relevance[key] >= 2 for key in keys)/k,
                   f'pooled_recall@{k}': hits/positives if positives else None,
                   f'ndcg@{k}': dcg/ideal if ideal else None})


def summarize(rows, k=10):
    if not rows:
        raise ValueError('No judged queries')
    groups = {'all': rows}
    for row in rows:
        for group in [row['market'], row['method'], f'{row["market"]}/{row["method"]}']:
            groups.setdefault(group, []).append(row)
    report = {}
    for group, subset in sorted(groups.items()):
        entry = dict(n_queries=len(subset), short_result_queries=sum(r['short_result'] for r in subset),
                     empty_result_queries=sum(r['returned'] == 0 for r in subset),
                     mean_returned=float(np.mean([r['returned'] for r in subset])))
        for metric in [f'ndcg@{k}', f'precision@{k}', f'pooled_recall@{k}', f'precision_grade_ge_2@{k}']:
            values = [r[metric] for r in subset if r[metric] is not None]
            entry[metric] = float(np.mean(values)) if values else None
            entry[metric+'_queries'] = len(values)
        report[group] = entry
    return report


def select_weights(caches, candidates, baseline, k=10):
    if not caches or any(cache.task['split'] != 'tune' for cache in caches):
        raise ValueError('Weight selection accepts only nonempty tune data')
    if len({cache.market for cache in caches}) != 1:
        raise ValueError('Select weights separately per market')
    rows = []
    for index, weights in enumerate(candidates):
        metrics = [query_metrics(cache, cache.rank(weights, k), k) for cache in caches]
        overall = summarize(metrics, k)['all']
        rows.append(dict(candidate_id=index, weights=weights, **overall))
    def key(row):
        distance = sum(abs(row['weights'][n] / sum(row['weights'].values()) -
                           baseline[n] / sum(baseline.values())) for n in WEIGHT_NAMES)
        return (row[f'ndcg@{k}'] if row[f'ndcg@{k}'] is not None else -1, -distance, -row['candidate_id'])
    best = max(rows, key=key)
    return best, rows


def bootstrap_delta(before, after, seed=20261105, samples=10000, k=10):
    """Descriptive paired-query interval, calculated after weight selection."""
    metric = f'ndcg@{k}'
    if [row['query_id'] for row in before] != [row['query_id'] for row in after]:
        raise ValueError('Bootstrap requires paired queries in the same order')
    differences = np.array([b[metric]-a[metric] for a, b in zip(before, after)
                            if a[metric] is not None and b[metric] is not None])
    if not len(differences):
        return dict(n_queries=0, mean_delta=None, percentile_95_interval=None)
    rng = np.random.default_rng(seed)
    means = differences[rng.integers(0, len(differences), size=(samples, len(differences)))].mean(axis=1)
    return dict(n_queries=len(differences), mean_delta=float(differences.mean()),
                percentile_95_interval=list(map(float, np.quantile(means, [.025, .975]))),
                bootstrap_samples=samples, seed=seed)
