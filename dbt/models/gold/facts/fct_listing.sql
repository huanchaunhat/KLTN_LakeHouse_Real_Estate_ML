-- Accumulating snapshot fact for the listing lifecycle.
-- Grain: one row per listing (listing_key), describing its latest state and
-- what happened to its price since it was first crawled.

with history as (
    select *
    from {{ ref('int_listing_enriched') }}
),

-- Crawl runs ordered by start time. Only full runs may tell that a listing
-- has disappeared; a partial run simply did not fetch it.
snapshots as (
    select
        snapshot_id,
        min(crawl_timestamp) as snapshot_started_at,
        max(cast(is_full_snapshot as int)) = 1 as is_full_snapshot
    from history
    group by snapshot_id
),

lifecycle as (
    select
        h.listing_key,
        count(*) as observation_count,
        sum(case when h.change_type = 'PRICE_CHANGED' then 1 else 0 end) as price_change_count,
        min_by(h.price_vnd, struct(h.crawl_timestamp, h.snapshot_id, h.observation_id)) as initial_price_vnd,
        min(h.crawl_timestamp) as first_seen_at,
        max(h.crawl_timestamp) as last_seen_at,
        max(s.snapshot_started_at) as last_snapshot_started_at
    from history h
    inner join snapshots s
        on h.snapshot_id = s.snapshot_id
    group by h.listing_key
),

-- A listing is removed at the first full snapshot that started after the
-- last snapshot it was seen in. Listings still present have no removal.
removals as (
    select
        l.listing_key,
        min_by(s.snapshot_id, s.snapshot_started_at) as removed_snapshot_id,
        min(s.snapshot_started_at) as removed_at
    from lifecycle l
    inner join snapshots s
        on s.is_full_snapshot
       and s.snapshot_started_at > l.last_snapshot_started_at
    group by l.listing_key
),

latest as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by listing_key
                order by crawl_timestamp desc, snapshot_id desc, _ingested_at desc, observation_id desc
            ) as latest_rank
        from history
    )
    where latest_rank = 1
)

select
    c.listing_key,
    c.observation_id as latest_observation_id,
    {{ location_key('c.province_id', 'c.district_id', 'c.ward_id') }} as location_key,
    {{ property_type_key('c.property_type', 'c.property_subtype') }} as property_type_key,
    {{ legal_status_key('c.legal_status') }} as legal_status_key,
    {{ project_key('c.project_name') }} as project_key,
    {{ date_key('c.posted_at') }} as posted_date_key,
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
    c.posted_at,
    r.removed_snapshot_id,
    r.removed_at,
    datediff(to_date(l.last_seen_at), to_date(l.first_seen_at)) as days_tracked,
    -- posted_at is missing for about half of the listings; fall back to first crawl.
    datediff(to_date(l.last_seen_at), to_date(coalesce(c.posted_at, l.first_seen_at))) as days_on_market,

    -- flags
    r.listing_key is null as is_active,
    c.is_market_eligible,
    c.is_price_training_eligible,
    c.is_price_outlier,
    c.outlier_reason
from latest c
inner join lifecycle l
    on c.listing_key = l.listing_key
left join removals r
    on c.listing_key = r.listing_key
