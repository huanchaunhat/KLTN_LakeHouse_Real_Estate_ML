"""Fetch one Nhà Tốt listing that is not in the model data yet.

Uses the same detail endpoint as scripts/crawl_nhatot.py and maps the payload
with the same rules as dbt staging (stg_nhatot_properties.sql), the property
category (dim_property_type.sql) and the price period
(ml_recommendation_features.sql), so a fetched listing has the same fields as
one read from the lakehouse.
"""
import hashlib
import re

import requests

DETAIL_URL = 'https://gateway.chotot.com/v1/public/ad-listing/{}'
SOURCE = 'nhatot'

CATEGORY = {'Nhà ở': 'HOUSE', 'Căn hộ/Chung cư': 'APARTMENT', 'Đất': 'LAND',
            'Phòng trọ': 'ROOM', 'Văn phòng, Mặt bằng kinh doanh': 'COMMERCIAL'}
# Listing link: www.nhatot.com/.../135220851.htm, www.chotot.com/..., or the gateway URL.
LINK = re.compile(r'^(?:https?://)?(?:[\w-]+\.)*(?:nhatot|chotot)\.com/.*?(\d{6,})(?:\.htm)?/?(?:[?#].*)?$', re.I)


session = requests.Session()
session.headers['User-Agent'] = 'Mozilla/5.0'


class NotFound(Exception):
    pass


def listing_id(text):
    """The Nhà Tốt id in a listing link or a bare id, else None."""
    text = text.strip()
    if text.isdigit():
        return text
    match = LINK.match(text)
    return match[1] if match else None


def fetch(list_id, timeout=15):
    try:
        response = session.get(DETAIL_URL.format(list_id), timeout=timeout)
    except requests.RequestException as exc:
        raise ConnectionError(f'Không kết nối được Nhà Tốt: {exc}') from exc
    if response.status_code in (400, 404):
        raise NotFound(list_id)
    if not response.ok:
        raise ConnectionError(f'Nhà Tốt trả lỗi {response.status_code}.')
    try:
        payload = response.json()
    except ValueError as exc:
        raise ConnectionError('Nhà Tốt trả dữ liệu không đọc được.') from exc
    if not payload.get('ad'):
        raise NotFound(list_id)
    return to_listing(list_id, payload)


def text(value):
    return value.strip() if isinstance(value, str) and value.strip() else None


def number(value, kind=float):
    try:
        return kind(value) if value is not None and value != '' else None
    except (TypeError, ValueError):
        return None


def param(payload, name):
    return (payload.get('ad_params') or {}).get(name, {}).get('value')


def to_listing(list_id, payload):
    ad = payload['ad']
    transaction = {'s': 'sale', 'u': 'rent'}.get(ad.get('type'), 'unknown')
    property_type = text(ad.get('category_name'))
    subtype = param(payload, 'apartment_type') or param(payload, 'house_type') or param(payload, 'land_type')
    legal = text(param(payload, 'property_legal_document'))
    traits = [t.strip() for t in (param(payload, 'pty_characteristics') or '').split(',') if t.strip()]
    price_raw = (ad.get('price_string') or '').lower()
    if transaction == 'sale':
        period = 'total'
    else:
        period = next((p for p, pattern in [('month', 'tháng|/th|month'), ('day', 'ngày|day'), ('year', 'năm|year')]
                       if re.search(pattern, price_raw)), 'unknown')
    as_id = lambda v: str(number(v, int)) if number(v, int) is not None else ''
    return {
        'listing_key': hashlib.sha256(f'{SOURCE}||{list_id}'.encode()).hexdigest(),
        'source_listing_id': str(list_id),
        'url': DETAIL_URL.format(list_id),
        'title': ad.get('subject'),
        'description': ad.get('body'),
        'price_vnd': number(ad.get('price')),
        'price_period': period,
        'area_m2': number(ad.get('size')),
        'living_area_m2': number(ad.get('living_size')),
        'width_m': number(ad.get('width')),
        'length_m': number(ad.get('length')),
        'bedrooms': number(ad.get('rooms'), int),
        'bathrooms': number(ad.get('toilets'), int),
        'total_floors': number(ad.get('floors'), int),
        'floor_number': number(ad.get('floornumber'), int),
        'province': text(ad.get('region_name')),
        'district': text(ad.get('area_name')),
        'ward': text(ad.get('ward_name')),
        'province_id': as_id(ad.get('region_v2')),
        'district_id': as_id(ad.get('area_v2')),
        'ward_id': as_id(ad.get('ward')),
        'transaction_type': transaction,
        'property_type': property_type,
        'property_category': CATEGORY.get(property_type, 'OTHER'),
        'property_subtype': subtype or property_type,
        'project_name': text(ad.get('pty_project_name')),
        # Some listings carry a raw option id ('1', '3') instead of a label.
        'legal_status': legal if legal and not legal.isdigit() else None,
        'furnishing': param(payload, 'furnishing_sell') or param(payload, 'furnishing_rent'),
        'house_direction': text(param(payload, 'direction')),
        'handover_status': param(payload, 'property_status'),
        'has_car_alley': 'Hẻm xe hơi' in traits,
        'is_frontage': 'Mặt tiền' in traits or subtype == 'Nhà mặt phố, mặt tiền',
        'is_wide_back': 'Nhà nở hậu' in traits or 'Nở hậu' in traits,
        'is_narrow_back': 'Nhà tóp hậu' in traits,
        'is_dilapidated': 'Nhà nát' in traits,
        'is_not_completed': 'Nhà chưa hoàn công' in traits,
        'is_planning_affected': 'Nhà dính quy hoạch / lộ giới' in traits,
        # Not in the model data: the page uses criteria search instead of "similar".
        'live': True,
    }
