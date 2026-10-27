"""CPU retrieval with mandatory market/category/unit filters and transparent scores."""
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import sklearn
from scipy.sparse import load_npz

from .contract import (SCHEMA_VERSION, CATEGORIES, PERIODS, prepare, eligible, utc,
                       file_hash, key_hash, identifier, normalize_text)


def numeric_similarity(values, target, width):
    values = np.asarray(values, dtype=float)
    out = np.zeros(len(values))
    if not np.isfinite(target) or target <= 0:
        return out
    valid = np.isfinite(values) & (values > 0)
    out[valid] = np.exp(-np.abs(np.log(values[valid] / target)) / width)
    return out


def location_similarity(candidates, seed):
    out = np.zeros(len(candidates))
    province = candidates.province_id.eq(seed.province_id).to_numpy() & bool(seed.province_id)
    out[province] = .25
    trusted = candidates.location_trusted.to_numpy() & bool(seed.location_trusted)
    district = province & trusted & candidates.district_id.eq(seed.district_id).to_numpy() & bool(seed.district_id)
    out[district] = .7
    ward = district & candidates.ward_id.eq(seed.ward_id).to_numpy() & bool(seed.ward_id)
    out[ward] = 1.
    lat, lon = candidates.latitude.to_numpy(), candidates.longitude.to_numpy()
    valid = trusted & np.isfinite(lat) & np.isfinite(lon) & (abs(lat) <= 90) & (abs(lon) <= 180)
    if np.isfinite(seed.latitude) and np.isfinite(seed.longitude) and abs(seed.latitude) <= 90 and abs(seed.longitude) <= 180:
        dlat = np.radians(lat[valid] - seed.latitude)
        dlon = np.radians(lon[valid] - seed.longitude)
        a = np.sin(dlat / 2)**2 + np.cos(np.radians(seed.latitude)) * np.cos(np.radians(lat[valid])) * np.sin(dlon / 2)**2
        km = 6371.0088 * 2 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))
        out[valid] = np.exp(-km / 12.)
    return out


class RecommendationEngine:
    def __init__(self, bundle):
        self.bundle = Path(bundle)
        self.manifest = json.loads((self.bundle / 'manifest.json').read_text())
        if self.manifest['schema_version'] != SCHEMA_VERSION:
            raise ValueError('Unsupported artifact schema')
        if self.manifest['sklearn'] != sklearn.__version__:
            raise ValueError('scikit-learn version differs from training; use the recorded environment')
        self.markets = {}
        self.lookup = {}
        for market in ['sale', 'rent']:
            folder = self.bundle / market
            meta = self.manifest['markets'][market]
            expected = {'catalog.parquet', 'matrix.npz', 'vectorizer.joblib'}
            if set(meta['files']) != expected:
                raise ValueError('Incomplete artifact bundle')
            for name, digest in meta['files'].items():
                if file_hash(folder / name) != digest:
                    raise ValueError(f'Artifact checksum mismatch: {market}/{name}')
            df = prepare(pd.read_parquet(folder / 'catalog.parquet'))
            X = load_npz(folder / 'matrix.npz')
            vec = joblib.load(folder / 'vectorizer.joblib')
            if (X.shape != (len(df), len(vec.vocabulary_)) or len(df) != meta['rows']
                    or X.shape[1] != meta['features'] or key_hash(df.listing_key) != meta['key_order_sha256']
                    or not df.transaction_type.eq(market).all()):
                raise ValueError('Catalog / index / vocabulary alignment mismatch')
            weights = self.manifest['weights'][market]
            if set(weights) != {'text', 'price', 'area', 'location', 'structure', 'freshness'} or any(
                    not np.isfinite(v) or v <= 0 for v in weights.values()):
                raise ValueError('Invalid scoring weights')
            self.markets[market] = (df, X, vec)
            for i, key in enumerate(df.listing_key):
                if key in self.lookup:
                    raise ValueError('Duplicate key across markets')
                self.lookup[key] = (market, i)

    def _candidates(self, market, category, period, as_of, max_age_days,
                    province_id=None, district_id=None, max_price=None, min_area=None,
                    min_bedrooms=None):
        if market not in self.markets or category not in CATEGORIES:
            raise ValueError('Specify sale/rent and a canonical property_category')
        if period not in PERIODS or (market == 'sale' and period != 'total') or (market == 'rent' and period == 'total'):
            raise ValueError('Specify a valid price_period (sale: total; rent: month/day/year/unknown)')
        df = self.markets[market][0]
        mask = eligible(df, as_of, max_age_days) & df.property_category.eq(category) & df.price_period.eq(period)
        for col, value in [('province_id', province_id), ('district_id', district_id)]:
            if value is not None:
                if not identifier(value):
                    raise ValueError(f'{col} must not be blank')
                mask &= df[col].eq(identifier(value))
                if col == 'district_id':
                    mask &= df.location_trusted
        for value, name in [(max_price, 'max_price'), (min_area, 'min_area')]:
            if value is not None and (not np.isfinite(value) or value <= 0):
                raise ValueError(f'{name} must be finite and positive')
        if max_price is not None:
            if period == 'unknown':
                raise ValueError('A known price period is required for budget constraints')
            mask &= df.price_vnd.le(max_price)
        if min_area is not None:
            mask &= df.area_m2.ge(min_area)
        if min_bedrooms is not None:
            if not np.isfinite(min_bedrooms) or min_bedrooms < 0:
                raise ValueError('min_bedrooms must be finite and nonnegative')
            mask &= df.bedrooms.ge(min_bedrooms)
        return np.flatnonzero(mask.to_numpy())

    def _rank(self, market, ids, parts, as_of, limit, exclude_group=None):
        if not isinstance(limit, int) or not 1 <= limit <= 50:
            raise ValueError('limit must be an integer in [1, 50]')
        df = self.markets[market][0]
        weights = self.manifest['weights'][market]
        ages = (utc(as_of) - df.iloc[ids].last_seen_at).dt.total_seconds().to_numpy() / 86400.
        parts['freshness'] = np.exp(-np.maximum(ages, 0) / 60.)
        denominator = sum(weights[k] for k in parts)
        scores = sum(weights[k] * v for k, v in parts.items()) / denominator
        order = np.argsort(-scores, kind='stable')
        used = {exclude_group} if exclude_group else set()
        output = []
        for j in order:
            row = df.iloc[ids[j]]
            if row.duplicate_group in used:
                continue
            used.add(row.duplicate_group)
            output.append(dict(
                listing_key=row.listing_key, title=row.title, url=row.url,
                transaction_type=market, property_category=row.property_category,
                price_vnd=float(row.price_vnd), price_period=row.price_period,
                area_m2=float(row.area_m2), province=row.province, district=row.district,
                score=round(float(scores[j]), 6),
                reasons={k: round(float(v[j]), 6) for k, v in parts.items()},
                dataset_version=self.manifest['dataset_version']))
            if len(output) == limit:
                break
        return output

    def similar(self, listing_key, *, limit=10, as_of=None, max_age_days=90):
        if listing_key not in self.lookup:
            raise KeyError(listing_key)
        now = utc(as_of)
        market, i = self.lookup[listing_key]
        df, X, _ = self.markets[market]
        seed = df.iloc[i]
        if not seed.province_id:
            raise ValueError('Seed has no province_id; cannot enforce locality')
        ids = self._candidates(market, seed.property_category, seed.price_period, now,
                               max_age_days, province_id=seed.province_id)
        ids = ids[df.iloc[ids].duplicate_group.ne(seed.duplicate_group).to_numpy()]
        candidates = df.iloc[ids]
        parts = dict(text=(X[ids] @ X[i].T).toarray().ravel(),
                     area=numeric_similarity(candidates.area_m2, seed.area_m2, .30),
                     location=location_similarity(candidates, seed))
        if seed.price_period != 'unknown' and seed.price_vnd > 0:
            parts['price'] = numeric_similarity(candidates.price_vnd, seed.price_vnd, .35)
        structure = []
        for col in ['bedrooms', 'bathrooms']:
            if pd.notna(seed[col]):
                vals = candidates[col].to_numpy()
                structure.append(np.where(np.isfinite(vals), np.exp(-abs(vals-seed[col])), 0.))
        for col in ['property_subtype', 'legal_status', 'furnishing']:
            if seed[col]:
                structure.append(candidates[col].map(normalize_text).eq(normalize_text(seed[col])).to_numpy().astype(float))
        if structure:
            parts['structure'] = np.mean(structure, axis=0)
        return self._rank(market, ids, parts, now, limit, seed.duplicate_group)

    def preferences(self, *, transaction_type, property_category, price_period=None,
                    province_id=None, district_id=None, max_price=None, min_area=None,
                    min_bedrooms=None, query=None, target_price=None, target_area=None,
                    limit=10, as_of=None, max_age_days=90):
        now = utc(as_of)
        if transaction_type == 'sale' and price_period is None:
            price_period = 'total'
        ids = self._candidates(transaction_type, property_category, price_period, now,
                               max_age_days, province_id, district_id, max_price, min_area, min_bedrooms)
        df, X, vec = self.markets[transaction_type]
        parts = {}
        if query is not None:
            q = vec.transform([normalize_text(query)])
            if not q.nnz:
                return []
            scores = (X[ids] @ q.T).toarray().ravel()
            matches = scores > 0
            ids = ids[matches]
            parts['text'] = scores[matches]
        for name, target, col, width in [('price', target_price, 'price_vnd', .35), ('area', target_area, 'area_m2', .30)]:
            if target is not None:
                if not np.isfinite(target) or target <= 0:
                    raise ValueError('Targets must be finite and positive')
                if name == 'price' and price_period == 'unknown':
                    raise ValueError('Cannot compare prices with unknown period')
                parts[name] = numeric_similarity(df.iloc[ids][col], target, width)
        return self._rank(transaction_type, ids, parts, now, limit)
