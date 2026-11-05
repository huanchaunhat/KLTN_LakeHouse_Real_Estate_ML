"""Create reproducible synthetic queries and blind annotation pools; no labels."""
import argparse
from collections import Counter
import csv
import hashlib
import json
from pathlib import Path
import random
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from recommendation.contract import eligible, file_hash
from recommendation.engine import RecommendationEngine


NAMES = dict(HOUSE='nhà ở', APARTMENT='căn hộ', LAND='đất', ROOM='phòng trọ',
             COMMERCIAL='mặt bằng kinh doanh')
COUNTS = {'sale': dict(HOUSE=24, APARTMENT=18, LAND=12, COMMERCIAL=6),
          'rent': dict(HOUSE=14, APARTMENT=18, ROOM=16, COMMERCIAL=10, LAND=2)}
TEXTS = {
    'HOUSE': ['hẻm xe hơi', 'nhà mặt tiền kinh doanh', 'nhà có sổ hồng riêng',
              'nhà khu dân cư yên tĩnh', 'nhà gần chợ trường học', 'nhà mới có nội thất'],
    'APARTMENT': ['căn hộ có ban công', 'căn hộ đầy đủ nội thất', 'căn hộ có bảo vệ',
                  'căn hộ gần trường học', 'căn hộ có hồ bơi', 'căn hộ thoáng mát'],
    'LAND': ['đất thổ cư sổ riêng', 'đất mặt tiền đường', 'đất khu dân cư',
             'đất đường xe hơi', 'đất gần chợ', 'đất vị trí thuận tiện'],
    'ROOM': ['phòng có máy lạnh', 'phòng có gác', 'phòng có cửa sổ',
             'phòng không chung chủ', 'phòng có chỗ để xe', 'phòng đầy đủ nội thất'],
    'COMMERCIAL': ['mặt bằng mặt tiền', 'văn phòng có thang máy', 'mặt bằng kinh doanh',
                   'kho đường xe tải', 'mặt bằng gần chợ', 'văn phòng có chỗ đậu xe'],
}


def dump(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')


def write_lines(path, records):
    path.write_text(''.join(json.dumps(x, ensure_ascii=False, allow_nan=False) + '\n'
                            for x in records), encoding='utf-8')


def write_csv(path, records):
    with path.open('w', encoding='utf-8-sig', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(records[0]))
        w.writeheader()
        w.writerows(records)


def select_sources(df, category, count, used, rng):
    """Round robin across district and category-relative price quartile."""
    d = df.loc[df.property_category.eq(category) & df.location_trusted
               & df.district_id.ne('') & ~df.duplicate_group.isin(used)].copy()
    d['quartile'] = pd.qcut(d.price_vnd.rank(method='first'), 4, labels=False)
    buckets = []
    for _, group in d.groupby(['district_id', 'quartile'], sort=True):
        ids = list(group.index)
        rng.shuffle(ids)
        buckets.append(ids)
    rng.shuffle(buckets)
    result = []
    while len(result) < count:
        advanced = False
        for bucket in buckets:
            while bucket and df.loc[bucket[-1], 'duplicate_group'] in used:
                bucket.pop()
            if bucket:
                i = bucket.pop()
                used.add(df.loc[i, 'duplicate_group'])
                result.append((i, int(d.loc[i, 'quartile']) + 1))
                advanced = True
                if len(result) == count:
                    break
        if not advanced:
            raise ValueError(f'Not enough independent sources for {category}')
    return result


def build(bundle, output, seed):
    if output.exists():
        raise ValueError('Output directory already exists; choose a new directory')
    engine = RecommendationEngine(bundle)
    rng = random.Random(seed)
    as_of = engine.manifest['as_of']
    tasks, query_rows, judge_rows, audit = [], [], [], []
    used = set()
    original_weights = {m: dict(w) for m, w in engine.manifest['weights'].items()}
    for market in ['sale', 'rent']:
        df = engine.markets[market][0]
        valid = df.loc[eligible(df, as_of, 90)].copy()
        for method in ['similar', 'preferences']:
            for category, count in COUNTS[market].items():
                selected = select_sources(valid, category, count, used, rng)
                # Each category/method is balanced between tune and test.
                for j, (idx, quartile) in enumerate(selected):
                    row = df.loc[idx]
                    split = 'tune' if j % 2 == 0 else 'test'
                    qid = f'{market}_{method}_{category.lower()}_{j+1:03d}'
                    label = f'{"Mua" if market == "sale" else "Thuê"} {NAMES[category]}'
                    if method == 'similar':
                        request = dict(listing_key=row.listing_key, max_age_days=90)
                        description = (f'Tôi quan tâm tin mẫu “{row.title}” tại {row.district}, '
                                       f'{row.province}; giá {row.price_vnd:,.0f} đồng'
                                       f'{"/tháng" if market == "rent" else ""}, '
                                       f'diện tích {row.area_m2:g} m². '
                                       'Tìm tin tương tự; cân nhắc nội dung, giá, diện tích, '
                                       'vị trí, cấu trúc và độ mới. Tin mẫu chỉ là sở thích tham chiếu; '
                                       'không bắt buộc cùng quận hoặc đúng giá mẫu.')
                    else:
                        target_price = max(1, round(float(row.price_vnd) / (100000 if market == 'rent' else 10000000)) *
                                           (100000 if market == 'rent' else 10000000))
                        target_area = max(1, round(float(row.area_m2)))
                        request = dict(transaction_type=market, property_category=category,
                                       price_period='total' if market == 'sale' else 'month',
                                       province_id=row.province_id, target_price=target_price,
                                       target_area=target_area, max_age_days=90)
                        # Mix broad and district-constrained requests, plus hard constraints.
                        if j % 3 != 0:
                            request['district_id'] = row.district_id
                        if j % 4 in (0, 1):
                            request['max_price'] = float(np.ceil(target_price * 1.25))
                        if j % 4 in (0, 2):
                            request['min_area'] = float(max(1, np.floor(target_area * .7)))
                        if category in ('HOUSE', 'APARTMENT') and j % 3 == 1 and pd.notna(row.bedrooms) and row.bedrooms >= 1:
                            request['min_bedrooms'] = int(min(row.bedrooms, 3))
                        preference = TEXTS[category][(j + (3 if market == 'rent' else 0)) % len(TEXTS[category])]
                        request['query'] = preference
                        location = f'{row.district}, {row.province}' if 'district_id' in request else row.province
                        hard = [f'đúng loại {NAMES[category]} và khu vực {location}']
                        if 'max_price' in request:
                            hard.append(f'giá không quá {request["max_price"]:,.0f} đồng')
                        if 'min_area' in request:
                            hard.append(f'diện tích tối thiểu {request["min_area"]:g} m²')
                        if 'min_bedrooms' in request:
                            hard.append(f'ít nhất {request["min_bedrooms"]} phòng ngủ')
                        description = (f'{label} tại {location}, giá mong muốn {target_price:,.0f} đồng'
                                       f'{"/tháng" if market == "rent" else ""}, diện tích khoảng {target_area} m². '
                                       f'Ưu tiên: {preference}. Bắt buộc: {"; ".join(hard)}. '
                                       'Giá và diện tích mong muốn là ưu tiên mềm trong phạm vi điều kiện bắt buộc; '
                                       'cân nhắc thêm độ mới của tin.')
                    task = dict(query_id=qid, dataset_version=engine.manifest['dataset_version'],
                                split=split, as_of=as_of, method=method, request=request,
                                description_vi=description, origin='synthetic_catalog_grounded',
                                source_listing_key=row.listing_key, source_duplicate_group=row.duplicate_group,
                                source_price_quartile=quartile)
                    call = engine.similar if method == 'similar' else engine.preferences
                    variants = [original_weights[market],
                                dict(text=.8, price=.04, area=.04, location=.04, structure=.04, freshness=.04),
                                dict(text=.01, price=.4, area=.25, location=.2, structure=.1, freshness=.04)]
                    chosen, provenance = {}, {}
                    for name, weights in zip(['current', 'text_heavy', 'attribute_heavy'], variants):
                        engine.manifest['weights'][market] = weights
                        for candidate in call(**request, limit=10, as_of=as_of):
                            key = candidate['listing_key']
                            chosen[key] = candidate
                            provenance.setdefault(key, []).append(name)
                    engine.manifest['weights'][market] = original_weights[market]
                    if method == 'similar':
                        ids = engine._candidates(market, category, row.price_period, as_of, 90,
                                                 province_id=row.province_id)
                        ids = [i for i in ids if df.loc[i, 'duplicate_group'] != row.duplicate_group]
                    else:
                        names = ['province_id', 'district_id', 'max_price', 'min_area', 'min_bedrooms']
                        ids = list(engine._candidates(market, category, request['price_period'], as_of, 90,
                                                      **{k: request[k] for k in names if k in request}))
                    rng.shuffle(ids)
                    groups = set(df.loc[df.listing_key.isin(chosen), 'duplicate_group'])
                    for i in ids:
                        if len(chosen) >= 40:
                            break
                        candidate = df.loc[i]
                        if candidate.duplicate_group in groups:
                            continue
                        chosen[candidate.listing_key] = None
                        provenance[candidate.listing_key] = ['random_eligible']
                        groups.add(candidate.duplicate_group)
                    keys = list(chosen)
                    rng.shuffle(keys)
                    task['relevance'] = dict.fromkeys(keys)
                    task['pool_size'] = len(keys)
                    tasks.append(task)
                    query_rows.append(dict(query_id=qid, split=split, market=market, method=method,
                                           category=category, district=row.district if method == 'similar' or 'district_id' in request else '',
                                           source_price_quartile=quartile, description_vi=description,
                                           source_listing_key=row.listing_key, source_title=row.title,
                                           source_description=str(row.get('description', '')),
                                           source_url=row.url, pool_size=len(keys),
                                           request_json=json.dumps(request, ensure_ascii=False)))
                    by_key = df.set_index('listing_key')
                    for position, key in enumerate(keys, 1):
                        c = by_key.loc[key]
                        judge_rows.append(dict(query_id=qid, description_vi=description,
                                               position=position, listing_key=key, title=c.title,
                                               description=str(c.get('description', '')), price_vnd=c.price_vnd,
                                               price_period=c.price_period, area_m2=c.area_m2,
                                               province=c.province, district=c.district, ward=c.ward,
                                               bedrooms='' if pd.isna(c.bedrooms) else c.bedrooms,
                                               bathrooms='' if pd.isna(c.bathrooms) else c.bathrooms,
                                               legal_status=c.legal_status, furnishing=c.furnishing,
                                               last_seen_at=str(c.last_seen_at), url=c.url,
                                               grade='', annotator_id='', reason=''))
                    audit.append(dict(query_id=qid, eligible_count=len(ids),
                                      candidate_sources=provenance))
                    print(f'{len(tasks):03d}/240 {qid}: {len(keys)} candidates', flush=True)
    assert len(tasks) == 240
    assert len({t['source_duplicate_group'] for t in tasks}) == 240
    assert len({json.dumps((t['method'], t['request']), sort_keys=True) for t in tasks}) == 240
    signatures = Counter((t['request'].get('transaction_type', t['query_id'].split('_')[0]),
                          t['method'], t['split']) for t in tasks)
    assert set(signatures.values()) == {30}
    output.mkdir(parents=True)
    for split in ['tune', 'test']:
        write_lines(output / f'queries_{split}.jsonl', [t for t in tasks if t['split'] == split])
        write_csv(output / f'annotation_{split}.csv',
                  [r for r in judge_rows if next(t['split'] for t in tasks if t['query_id'] == r['query_id']) == split])
    write_csv(output / 'queries.csv', query_rows)
    (output / 'queries.md').write_text(
        '# 240 truy vấn gợi ý BĐS\n\n'
        'Kịch bản tổng hợp dựa trên snapshot TP.HCM; cần người đánh giá xác nhận. '
        'Chỉ dùng tune để chọn trọng số, giữ test cho đánh giá cuối.\n\n' +
        '\n\n'.join(f'## {i}. {t["query_id"]} ({t["split"]})\n\n'
                    f'{t["description_vi"]}\n\n'
                    f'Số ứng viên chấm: {t["pool_size"]}.' for i, t in enumerate(tasks, 1)) + '\n',
        encoding='utf-8')
    dump(output / 'pool_audit.json', audit)
    dump(output / 'manifest.json', dict(dataset_version=engine.manifest['dataset_version'], as_of=as_of,
         bundle=str(bundle), bundle_manifest_sha256=file_hash(bundle / 'manifest.json'), random_seed=seed,
         query_count=len(tasks), annotation_pairs=len(judge_rows), query_origin='synthetic_catalog_grounded',
         labels_status='unjudged', query_distribution=dict(Counter(f'{r["market"]}/{r["method"]}/{r["split"]}' for r in query_rows)),
         category_distribution=dict(Counter(f'{r["market"]}/{r["category"]}' for r in query_rows)),
         district_distribution=dict(Counter(r['district'] or 'province_wide' for r in query_rows)),
         short_pool_queries=[t['query_id'] for t in tasks if t['pool_size'] < 40]))


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--bundle', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--seed', type=int, default=20261103)
    a = p.parse_args()
    build(a.bundle, a.output, a.seed)
