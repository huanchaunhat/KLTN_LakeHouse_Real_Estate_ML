{{ config(
    partition_by=['event_date_key']
) }}

-- Transaction fact of listing lifecycle events.
-- Grain: one row per event of a listing:
--   NEW            first time the listing is crawled
--   PRICE_CHANGED  price differs from the previous observation
--   UPDATED        content changed, price unchanged
--   REMOVED        missing from a later full snapshot (see fct_listing.removed_at)
-- Source of the market supply / liquidity mart.

with history as (
    select *
    from {{ ref('int_listing_enriched') }}
),

-- Listings found by the first full crawl were already on the market before the
-- pipeline started, so their NEW events are not new supply.
-- snapshot_id is yyyyMMdd_HHmmss, so it sorts in crawl order.
initial_load as (
    select min(snapshot_id) as initial_load_snapshot_id
    from history
    where is_full_snapshot
),

listing_first_seen as (
    select listing_key, first_seen_at, posted_at
    from {{ ref('fct_listing') }}
),

observed_events as (
    select
        h.observation_id as event_id,
        h.listing_key,
        h.change_type as event_type,
        h.snapshot_id,
        h.crawl_timestamp as event_at,
        {{ location_key('h.province_id', 'h.district_id', 'h.ward_id') }} as location_key,
        {{ property_type_key('h.property_type', 'h.property_subtype') }} as property_type_key,
        {{ legal_status_key('h.legal_status') }} as legal_status_key,
        {{ project_key('h.project_name') }} as project_key,
        h.transaction_type,
        h.price_vnd,
        h.previous_price_vnd,
        h.price_change_vnd,
        h.price_change_pct,
        h.area_m2,
        h.price_per_m2,
        f.first_seen_at,
        f.posted_at,
        h.is_market_eligible,
        h.is_price_outlier
    from history h
    inner join listing_first_seen f
        on h.listing_key = f.listing_key
    where h.change_type <> 'UNCHANGED'
),

-- The removal event carries the last known state of the listing.
removed_events as (
    select
        sha2(concat(listing_key, '|REMOVED|', removed_snapshot_id), 256) as event_id,
        listing_key,
        'REMOVED' as event_type,
        removed_snapshot_id as snapshot_id,
        removed_at as event_at,
        location_key,
        property_type_key,
        legal_status_key,
        project_key,
        transaction_type,
        current_price_vnd as price_vnd,
        cast(null as bigint) as previous_price_vnd,
        cast(null as bigint) as price_change_vnd,
        cast(null as double) as price_change_pct,
        area_m2,
        current_price_per_m2 as price_per_m2,
        first_seen_at,
        posted_at,
        is_market_eligible,
        is_price_outlier
    from {{ ref('fct_listing') }}
    where removed_at is not null
),

events as (
    select * from observed_events
    union all
    select * from removed_events
)

select
    e.event_id,
    e.listing_key,
    e.event_type,
    e.snapshot_id,
    {{ date_key('e.event_at') }} as event_date_key,
    e.location_key,
    e.property_type_key,
    e.legal_status_key,
    e.project_key,

    -- degenerate dimensions
    e.transaction_type,
    e.event_at,

    -- measures
    e.price_vnd,
    e.previous_price_vnd,
    e.price_change_vnd,
    e.price_change_pct,
    e.area_m2,
    e.price_per_m2,
    datediff(to_date(e.event_at), to_date(e.first_seen_at)) as days_since_first_seen,
    -- posted_at is missing for about half of the listings; fall back to first crawl.
    datediff(to_date(e.event_at), to_date(coalesce(e.posted_at, e.first_seen_at))) as days_on_market,

    -- flags
    e.event_type = 'NEW' and e.snapshot_id <= i.initial_load_snapshot_id as is_initial_load,
    coalesce(e.price_change_vnd < 0, false) as is_price_decrease,
    coalesce(e.price_change_vnd > 0, false) as is_price_increase,
    e.is_market_eligible,
    e.is_price_outlier
from events e
cross join initial_load i
