{{ config(
    materialized='view',
    schema='silver'
) }}

-- Gold-ready valid observations.
-- Adds the business rules that Gold needs but Silver deliberately does not
-- enforce: statistical outlier flags and snapshot completeness.
--
-- A view: it adds a few columns to the history from two small tables
-- (broadcast joins). Materializing it rewrote a full copy of the history
-- every day; as a view each Gold model reads only the columns it needs.

with history as (
    select *
    from {{ ref('listing_history') }}
),

-- Snapshot order, completeness and initial load (see int_crawl_snapshots).
snapshots as (
    select snapshot_id, snapshot_started_at, is_full_snapshot, is_initial_load
    from {{ ref('int_crawl_snapshots') }}
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
        s.snapshot_started_at,
        s.is_full_snapshot,
        s.is_initial_load as is_initial_load_snapshot,

        case
            when h.price_vnd is null or h.price_vnd <= 0 then 'NON_POSITIVE_PRICE'
            when h.area_m2 is null or h.area_m2 <= 0 then 'NON_POSITIVE_AREA'
            when h.price_per_m2 < b.price_per_m2_lower
              or h.price_per_m2 > b.price_per_m2_upper then 'PRICE_PER_M2_OUT_OF_RANGE'
            when h.area_m2 < b.area_m2_lower
              or h.area_m2 > b.area_m2_upper then 'AREA_OUT_OF_RANGE'
        end as outlier_reason
    from history h
    inner join snapshots s
        on h.snapshot_id = s.snapshot_id
    left join outlier_bounds b
        on h.transaction_type = b.transaction_type
       and h.property_type <=> b.property_type
)

select
    *,
    outlier_reason is not null as is_price_outlier
from enriched
