-- Accumulating snapshot fact for the listing lifecycle.
-- Grain: one row per listing (listing_key), describing its latest state and
-- what happened to its price since it was first crawled.

with history as (
    select *
    from {{ ref('int_listing_enriched') }}
),

lifecycle as (
    select
        listing_key,
        count(*) as observation_count,
        sum(case when change_type = 'PRICE_CHANGED' then 1 else 0 end) as price_change_count,
        min_by(price_vnd, struct(crawl_timestamp, snapshot_id, observation_id)) as initial_price_vnd,
        min(crawl_timestamp) as first_seen_at,
        -- Earliest post time seen, in case a push was observed before
        -- orig_list_time was filled in.
        min(posted_at) as posted_at,
        max(crawl_timestamp) as last_seen_at,
        -- Stock found by the initial load was already on the market before the
        -- pipeline started, so it is not new supply.
        min_by(is_initial_load_snapshot, snapshot_started_at) as is_initial_inventory
    from history
    group by listing_key
),

-- Removal is decided in Silver from every observation, including rate-limited
-- and quarantined ones (see int_listing_removals).
removals as (
    select *
    from {{ ref('int_listing_removals') }}
),

-- The latest observation of each listing is already picked in Silver
-- (same ordering), so a semi join replaces a sort of the whole history.
latest as (
    select h.*
    from history h
    left semi join {{ ref('listing_current') }} c
        on h.observation_id = c.observation_id
)

select
    c.listing_key,
    c.observation_id as latest_observation_id,
    {{ location_key('c.province_id', 'c.district_id', 'c.ward_id') }} as location_key,
    {{ property_type_key('c.property_type', 'c.property_subtype') }} as property_type_key,
    {{ legal_status_key('c.legal_status') }} as legal_status_key,
    {{ project_key('c.project_name') }} as project_key,
    {{ date_key('l.posted_at') }} as posted_date_key,
    {{ date_key('l.first_seen_at') }} as first_seen_date_key,
    {{ date_key('l.last_seen_at') }} as last_seen_date_key,
    {{ date_key('r.removed_at') }} as removed_date_key,

    c.transaction_type,

    -- current measures
    c.price_vnd as current_price_vnd,
    c.area_m2,
    c.price_per_m2 as current_price_per_m2,

    -- lifecycle measures
    l.initial_price_vnd,
    c.price_vnd - l.initial_price_vnd as total_price_change_vnd,
    case
        when l.initial_price_vnd > 0
            then (cast(c.price_vnd as double) - l.initial_price_vnd) / l.initial_price_vnd * 100.0
    end as total_price_change_pct,
    l.price_change_count,
    l.observation_count,
    l.first_seen_at,
    l.last_seen_at,
    l.posted_at,
    r.removed_snapshot_id,
    r.removed_at,
    r.removal_evidence,
    datediff(to_date(l.last_seen_at), to_date(l.first_seen_at)) as days_tracked,
    datediff(to_date(l.last_seen_at), to_date(coalesce(l.posted_at, l.first_seen_at))) as days_on_market,

    -- flags
    r.listing_key is null as is_active,
    l.is_initial_inventory,
    c.is_market_eligible,
    c.is_price_training_eligible,
    c.is_price_outlier,
    c.outlier_reason
from latest c
inner join lifecycle l
    on c.listing_key = l.listing_key
left join removals r
    on c.listing_key = r.listing_key
