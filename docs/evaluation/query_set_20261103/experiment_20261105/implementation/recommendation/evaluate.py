"""Evaluate a fixed judged pool; metrics are NOT click-through estimates."""
import argparse
import json
import math
from pathlib import Path

import numpy as np

from .engine import RecommendationEngine


def evaluate(bundle, labels, k=10):
    tasks = [json.loads(line) for line in Path(labels).read_text().splitlines() if line.strip()]
    return evaluate_engine(RecommendationEngine(bundle), tasks, k)


def evaluate_engine(engine, tasks, k=10):
    """Evaluate with an already loaded engine (same formula as the CLI)."""
    if not isinstance(k, int) or not 1 <= k <= 50:
        raise ValueError('k must be an integer in [1, 50]')
    rows = []
    for task in tasks:
        if task['dataset_version'] != engine.manifest['dataset_version']:
            raise ValueError('Annotation catalog version does not match the bundle')
        if task['split'] not in ('tune', 'test'):
            raise ValueError('Each annotation must identify its tune/test split')
        relevance = task['relevance']
        if not relevance or any(type(v) is not int or not 0 <= v <= 3 for v in relevance.values()):
            raise ValueError('relevance must map listing keys to integer grades 0..3')
        args = dict(task['request'])
        args.update(limit=k, as_of=task['as_of'])
        if task['method'] == 'similar':
            output = engine.similar(**args)
            market, seed_i = engine.lookup[args['listing_key']]
            catalog = engine.markets[market][0]
            seed = catalog.iloc[seed_i]
            candidates = engine._candidates(market, seed.property_category, seed.price_period,
                                            args['as_of'], args.get('max_age_days', 90),
                                            province_id=seed.province_id)
            pool = catalog.iloc[candidates]
            pool = pool.loc[pool.duplicate_group.ne(seed.duplicate_group)]
        elif task['method'] == 'preferences':
            output = engine.preferences(**args)
            market = args['transaction_type']
            filter_names = ['province_id', 'district_id', 'max_price', 'min_area', 'min_bedrooms']
            candidates = engine._candidates(market, args['property_category'],
                                            args.get('price_period', 'total' if market == 'sale' else None),
                                            args['as_of'], args.get('max_age_days', 90),
                                            **{k: args[k] for k in filter_names if k in args})
            pool = engine.markets[market][0].iloc[candidates]
        else:
            raise ValueError('Unknown evaluation method')
        valid_keys = set(pool.listing_key)
        if any(key not in engine.lookup or (grade > 0 and key not in valid_keys)
               for key, grade in relevance.items()):
            raise ValueError('Judgments contain unknown IDs or positives outside the eligible pool')
        keys = [x['listing_key'] for x in output]
        if any(key not in relevance for key in keys):
            raise ValueError('Unjudged Top-K result: extend the pool before comparing models')
        positives = sum(v > 0 for v in relevance.values())
        gains = [2**relevance[key] - 1 for key in keys]
        dcg = sum(g / math.log2(i + 2) for i, g in enumerate(gains))
        ideal = sum((2**v-1) / math.log2(i+2) for i, v in enumerate(sorted(relevance.values(), reverse=True)[:k]))
        hits = sum(relevance[key] > 0 for key in keys)
        rows.append(dict(market=market, split=task['split'], precision=hits/k,
                         pooled_recall=hits/positives if positives else None,
                         ndcg=dcg/ideal if ideal else None))
    if not rows:
        raise ValueError('No judged tasks')
    result = {}
    for market, split in sorted({(r['market'], r['split']) for r in rows}):
        subset = [r for r in rows if r['market'] == market and r['split'] == split]
        report = {'n_queries': len(subset)}
        for metric in ['precision', 'pooled_recall', 'ndcg']:
            values = [r[metric] for r in subset if r[metric] is not None]
            report[f'{metric}@{k}'] = float(np.mean(values)) if values else None
            report[f'{metric}_queries'] = len(values)
        result[f'{market}/{split}'] = report
    return result


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--bundle', required=True)
    p.add_argument('--labels', required=True)
    p.add_argument('--k', type=int, default=10)
    p.add_argument('--output', type=Path)
    a = p.parse_args()
    text = json.dumps(evaluate(a.bundle, a.labels, a.k), indent=2)
    if a.output:
        a.output.write_text(text)
    print(text)
