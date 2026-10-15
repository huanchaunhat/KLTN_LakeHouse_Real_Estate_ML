{{ config(
    materialized='table',
    file_format='delta',
    schema='silver'
) }}

-- Gold-ready valid observations.
-- Adds the business rules that Gold needs but Silver deliberately does not
-- enforce: statistical outlier flags and snapshot completeness.

with history as (
    select *
    from {{ ref('listing_history') }}
),

-- A crawl run can be partial (e.g. a retry that only re-fetched a few
-- listings). Only "full" snapshots may be used to decide whether a listing
-- is still on the market.
snapshot_stats as (
    select
        snapshot_id,
        count(distinct listing_key) as snapshot_listing_count
    from history
    group by snapshot_id
),

full_snapshots as (
    select snapshot_id
    from snapshot_stats
    where snapshot_listing_count >= {{ var('full_snapshot_min_ratio') }} * (
        select max(snapshot_listing_count) from snapshot_stats
    )
),

-- Outlier bounds are computed per market segment on the latest state of each
-- listing, so a listing observed many times does not weigh more.
outlier_bounds as (
    select
        transaction_type,
        property_type,
        percentile_approx(price_per_m2, {{ var('outlier_lower_quantile') }}) as price_per_m2_lower,
        percentile_approx(price_per_m2, {{ var('outlier_upper_quantile') }}) as price_per_m2_upper,
        percentile_approx(area_m2, {{ var('outlier_lower_quantile') }}) as area_m2_lower,
        percentile_approx(area_m2, {{ var('outlier_upper_quantile') }}) as area_m2_upper
    from {{ ref('listing_current') }}
    where price_per_m2 > 0
    group by transaction_type, property_type
),

enriched as (
    select
        h.*,
        f.snapshot_id is not null as is_full_snapshot,

        case
            when h.price_vnd is null or h.price_vnd <= 0 then 'NON_POSITIVE_PRICE'
            when h.area_m2 is null or h.area_m2 <= 0 then 'NON_POSITIVE_AREA'
            when h.price_per_m2 < b.price_per_m2_lower
              or h.price_per_m2 > b.price_per_m2_upper then 'PRICE_PER_M2_OUT_OF_RANGE'
            when h.area_m2 < b.area_m2_lower
              or h.area_m2 > b.area_m2_upper then 'AREA_OUT_OF_RANGE'
        end as outlier_reason
    from history h
    left join full_snapshots f
        on h.snapshot_id = f.snapshot_id
    left join outlier_bounds b
        on h.transaction_type = b.transaction_type
       and h.property_type <=> b.property_type
)

select
    *,
    outlier_reason is not null as is_price_outlier
from enriched
