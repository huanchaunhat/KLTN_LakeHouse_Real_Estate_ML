"""Mechanical checks and direct-engine timings, not human relevance evaluation."""
import argparse
import json
import resource
import time
from pathlib import Path

import numpy as np

from .contract import eligible, utc
from .engine import RecommendationEngine


def benchmark(bundle, queries=50):
    if queries < 1:
        raise ValueError('queries must be positive')
    start = time.perf_counter()
    engine = RecommendationEngine(bundle)
    loaded = time.perf_counter() - start
    now = utc(engine.manifest['as_of'])
    report = dict(dataset_version=engine.manifest['dataset_version'], as_of=now.isoformat(),
                  evaluation_type='mechanics_only_no_relevance_labels', load_seconds=round(loaded, 3), markets={})
    for market, (df, _, _) in engine.markets.items():
        seeds = df.loc[df.province_id.ne('')].sample(n=min(queries, int(df.province_id.ne('').sum())), random_state=42)
        times, preference_times = [], []
        results = violations = pref_results = pref_violations = short = 0
        example = None
        for _, seed in seeds.iterrows():
            t = time.perf_counter()
            hits = engine.similar(seed.listing_key, as_of=now)
            times.append((time.perf_counter() - t)*1000)
            short += len(hits) < 10
            groups = set()
            for hit in hits:
                r = df.iloc[engine.lookup[hit['listing_key']][1]]
                results += 1
                violations += int(not (
                    hit['transaction_type'] == market and r.property_category == seed.property_category
                    and r.price_period == seed.price_period and r.province_id == seed.province_id
                    and r.listing_key != seed.listing_key and r.duplicate_group != seed.duplicate_group
                    and r.duplicate_group not in groups
                    and bool(eligible(df.iloc[[engine.lookup[hit['listing_key']][1]]], now, 90).iloc[0])))
                groups.add(r.duplicate_group)
            if example is None and hits:
                example = dict(seed=seed.listing_key, recommendations=hits[:3])
            if seed.price_period == 'unknown':
                continue
            filters = dict(transaction_type=market, property_category=seed.property_category,
                           price_period=seed.price_period, province_id=seed.province_id,
                           max_price=float(seed.price_vnd)*1.1, min_area=float(seed.area_m2)*.9,
                           min_bedrooms=1, as_of=now)
            t = time.perf_counter()
            hits = engine.preferences(**filters)
            preference_times.append((time.perf_counter()-t)*1000)
            for hit in hits:
                r = df.iloc[engine.lookup[hit['listing_key']][1]]
                pref_results += 1
                pref_violations += int(not (
                    hit['transaction_type'] == market and r.property_category == seed.property_category
                    and r.price_period == seed.price_period and r.province_id == seed.province_id
                    and r.price_vnd <= filters['max_price'] and r.area_m2 >= filters['min_area']
                    and r.bedrooms >= 1))
        report['markets'][market] = dict(
            rows=len(df), similar_queries=len(times), similar_results=results,
            similar_violations=violations, underfilled_similar_queries=short,
            similar_p95_ms=round(float(np.percentile(times, 95)), 2) if times else None,
            preference_queries=len(preference_times), preference_results=pref_results,
            preference_violations=pref_violations,
            preference_p95_ms=round(float(np.percentile(preference_times, 95)), 2) if preference_times else None,
            example=example)
        if violations or pref_violations:
            raise AssertionError(f'Constraint violations in {market}')
    # macOS reports bytes; Linux reports KiB.
    import sys
    divisor = 1024**2 if sys.platform == 'darwin' else 1024
    report['peak_process_rss_mb'] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/divisor, 1)
    report['total_seconds'] = round(time.perf_counter()-start, 2)
    return report


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--bundle', required=True)
    p.add_argument('--queries', type=int, default=50, help='Queries PER market')
    p.add_argument('--output', required=True, type=Path)
    a = p.parse_args()
    result = benchmark(a.bundle, a.queries)
    a.output.parent.mkdir(parents=True, exist_ok=True)
    a.output.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    print(json.dumps({**result, 'markets': {k: {x: y for x, y in v.items() if x != 'example'}
                                         for k, v in result['markets'].items()}}, indent=2))
