-- depends_on: {{ ref('int_listing_changes') }}

{{ config(
    materialized='incremental',
    unique_key='observation_id',
    incremental_strategy='merge',
    partition_by=['snapshot_date_key'],
    merge_partition_column='snapshot_date_key',
    on_schema_change='append_new_columns'
) }}

-- Periodic snapshot fact: the price of every listing at every crawl.
-- Grain: one row per valid listing observation (listing x crawl snapshot).
-- Source of the weekly/monthly price-trend marts.
--
-- Incremental: only observations (re)written in Silver since the last run are
-- merged. is_price_outlier / outlier_reason are therefore point-in-time: they
-- use the outlier bounds of the day the observation was loaded and do not
-- move afterwards. dbt run --full-refresh recomputes them with today's bounds.
--
-- int_listing_changes is only ref()'d inside the is_incremental() branch below,
-- so dbt cannot infer the dependency statically.
-- depends_on: {{ ref('int_listing_changes') }}

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
    outlier_reason,
    changes_loaded_at
from {{ ref('int_listing_enriched') }}
{% if is_incremental() %}
where {{ incremental_batch_predicate(ref('int_listing_changes'), 'changes_loaded_at') }}
{% endif %}
