"""Shared, versioned input and scoring contract. All money is source-listed VND."""
import hashlib
import re
import unicodedata

import numpy as np
import pandas as pd

SCHEMA_VERSION = 1
CATEGORIES = {'HOUSE', 'APARTMENT', 'LAND', 'ROOM', 'COMMERCIAL', 'OTHER'}
PERIODS = {'total', 'month', 'day', 'year', 'unknown'}
REQUIRED = {
    'listing_key', 'transaction_type', 'property_category', 'price_period',
    'price_vnd', 'area_m2', 'last_seen_at', 'is_recommendable',
    'location_trusted', 'content_text', 'duplicate_group',
}
NUMERIC = ['price_vnd', 'area_m2', 'bedrooms', 'bathrooms', 'latitude', 'longitude']
IDS = ['province_id', 'district_id', 'ward_id']
TEXT = ['title', 'province', 'district', 'ward', 'url', 'property_subtype',
        'project_name', 'legal_status', 'furnishing']
# Starting baseline, NOT weights learned or validated against relevance labels.
DEFAULT_WEIGHTS = dict(text=.35, price=.25, area=.15, location=.15,
                       structure=.05, freshness=.05)


def normalize_text(value):
    if value is None or pd.isna(value):
        return ''
    value = unicodedata.normalize('NFKC', str(value)).casefold()
    # Accent folding also maps Vietnamese đ; applied identically to items/queries.
    value = unicodedata.normalize('NFD', value).replace('đ', 'd')
    value = ''.join(c for c in value if unicodedata.category(c) != 'Mn')
    return re.sub(r'\s+', ' ', value).strip()[:4000]


def identifier(value):
    if value is None or pd.isna(value):
        return ''
    return str(int(value)) if isinstance(value, (float, np.floating, int, np.integer)) else str(value).strip()


def utc(value=None):
    ts = pd.Timestamp.now(tz='UTC') if value is None else pd.Timestamp(value)
    if pd.isna(ts):
        raise ValueError('as_of must be a valid timestamp')
    return ts.tz_localize('UTC') if ts.tzinfo is None else ts.tz_convert('UTC')


def key_hash(keys):
    return hashlib.sha256('\n'.join(map(str, keys)).encode()).hexdigest()


def file_hash(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def prepare(df):
    missing = REQUIRED - set(df.columns)
    if missing:
        raise ValueError(f'Missing Gold columns: {sorted(missing)}')
    df = df.copy()
    for col in ['listing_key', 'transaction_type', 'property_category', 'price_period', 'duplicate_group']:
        if df[col].isna().any() or df[col].astype(str).str.strip().eq('').any():
            raise ValueError(f'Null/blank {col}')
        df[col] = df[col].astype(str).str.strip()
    if df.listing_key.duplicated().any():
        raise ValueError('Duplicate listing_key: fix the Gold snapshot')
    if not set(df.transaction_type) <= {'sale', 'rent'}:
        raise ValueError('transaction_type must be sale or rent')
    if not set(df.property_category) <= CATEGORIES or not set(df.price_period) <= PERIODS:
        raise ValueError('Invalid property category or price period')
    if ((df.transaction_type.eq('sale') & df.price_period.ne('total')) |
            (df.transaction_type.eq('rent') & df.price_period.eq('total'))).any():
        raise ValueError('Sale/rent price units do not match')
    for col in ['is_recommendable', 'location_trusted']:
        if df[col].isna().any() or not df[col].isin([True, False]).all():
            raise ValueError(f'{col} must contain booleans')
        df[col] = df[col].astype(bool)
    for col in NUMERIC:
        if col not in df:
            df[col] = np.nan
        df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
        df.loc[~np.isfinite(df[col]), col] = np.nan
    for col in IDS + TEXT:
        if col not in df:
            df[col] = ''
        df[col] = df[col].map(identifier) if col in IDS else df[col].fillna('').astype(str)
    df['last_seen_at'] = pd.to_datetime(df.last_seen_at, utc=True, errors='coerce')
    df['content_text'] = df.content_text.map(normalize_text)
    df.loc[df.content_text.eq(''), 'content_text'] = 'bat dong san'
    return df.reset_index(drop=True)


def eligible(df, as_of, max_age_days):
    if not isinstance(max_age_days, (int, np.integer)) or not 0 <= max_age_days <= 3650:
        raise ValueError('max_age_days must be an integer in [0, 3650]')
    now = utc(as_of)
    return (df.is_recommendable & df.price_vnd.gt(0) & df.area_m2.gt(0)
            & df.last_seen_at.le(now)
            & df.last_seen_at.ge(now - pd.Timedelta(days=int(max_age_days))))
