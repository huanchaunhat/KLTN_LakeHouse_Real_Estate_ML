{{ config(
    partition_by=['snapshot_date_key']
) }}

-- Periodic snapshot fact: the price of every listing at every crawl.
-- Grain: one row per valid listing observation (listing x crawl snapshot).
-- Source of the weekly/monthly price-trend marts.

select
    observation_id,
    listing_key,
    snapshot_id,
    {{ date_key('crawl_date') }} as snapshot_date_key,
    {{ location_key('province_id', 'district_id', 'ward_id') }} as location_key,
    {{ property_type_key('property_type', 'property_subtype') }} as property_type_key,
    {{ legal_status_key('legal_status') }} as legal_status_key,
    {{ project_key('project_name') }} as project_key,

    -- degenerate dimensions
    transaction_type,
    change_type,
    crawl_date,
    crawl_timestamp,

    -- measures
    price_vnd,
    area_m2,
    price_per_m2,
    price_change_vnd,
    price_change_pct,

    -- flags
    change_type = 'NEW' as is_new_listing,
    coalesce(price_change_vnd > 0, false) as is_price_increase,
    coalesce(price_change_vnd < 0, false) as is_price_decrease,
    is_market_eligible,
    is_price_outlier,
    outlier_reason
from {{ ref('int_listing_enriched') }}
