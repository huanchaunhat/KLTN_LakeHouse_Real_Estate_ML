-- depends_on: {{ ref('int_listing_changes') }}

{{ config(
    materialized='incremental',
    file_format='delta',
    unique_key='listing_key',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    schema='silver'
) }}

-- Latest valid observation of each listing. Grain: one row per listing_key.
--
-- Incremental: only listings with rows (re)written in int_listing_changes since
-- the last run are re-ranked. Those rows always include the listing's latest
-- observation (a late run recomputes the whole listing there).
--
-- int_listing_changes is only ref()'d inside the is_incremental() branch below,
-- so dbt cannot infer the dependency statically.
-- depends_on: {{ ref('int_listing_changes') }}

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
    {% if is_incremental() %}
    where {{ incremental_batch_predicate(ref('int_listing_changes'), 'changes_loaded_at') }}
    {% endif %}
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
    width_m,
    length_m,
    living_area_m2,
    house_direction,
    furnishing,
    handover_status,
    property_characteristics,
    is_broker,
    has_car_alley,
    is_frontage,
    is_wide_back,
    is_narrow_back,
    is_dilapidated,
    is_not_completed,
    is_planning_affected,
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
    transformed_at,
    changes_loaded_at
from ranked
where current_rank = 1
