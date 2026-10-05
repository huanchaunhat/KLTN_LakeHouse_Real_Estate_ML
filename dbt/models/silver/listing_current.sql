{{ config(
    materialized='table',
    file_format='delta',
    schema='silver'
) }}

with ranked as (
    select
        *,
        row_number() over (
            partition by listing_key
            order by
                crawl_timestamp desc,
                snapshot_id desc,
                _ingested_at desc,
                observation_id desc
        ) as current_rank
    from {{ ref('listing_history') }}
)

select
    listing_key,
    observation_id,
    source,
    source_listing_id,
    snapshot_id,
    crawl_date,
    crawl_timestamp,
    http_status,
    url,
    source_error,
    title,
    description,
    transaction_type,
    transaction_type_raw,
    price_vnd,
    price_in_billions,
    price_raw,
    area_m2,
    price_per_m2,
    property_type,
    property_subtype,
    bedrooms,
    bathrooms,
    total_floors,
    floor_number,
    address,
    street,
    ward,
    district,
    province,
    ward_id,
    district_id,
    province_id,
    latitude,
    longitude,
    legal_status,
    deposit_vnd,
    project_name,
    image_urls,
    image_count,
    phone_hash,
    source_status,
    source_state,
    posted_at,
    source_updated_at,
    content_hash,
    quality_errors,
    primary_quality_flag,
    data_quality_flag,
    is_valid_observation,
    is_market_eligible,
    is_price_training_eligible,
    previous_price_vnd,
    price_change_vnd,
    price_change_pct,
    first_seen_at,
    last_seen_at,
    change_type,
    listing_status,
    _source_file,
    _ingested_at,
    transformed_at
from ranked
where current_rank = 1
