-- Weekly market supply and liquidity by city / zone / district.
-- Grain: week x geo (city / zone / district) x transaction_type x property_type,
-- the same as agg_price_trend_weekly so the two marts can be joined.
--
-- - active_listing_count: listings observed during the week (stock on the market).
-- - new / removed: listings entering and leaving the market. NEW events of the
--   initial crawl are excluded because those listings were already on the market.
-- - Removal is only a proxy for a deal: a listing may also expire or be withdrawn.
-- - Every count is a count of distinct listings, so repeated crawls do not inflate it.
-- - A week may have removals but no active listing (the listing was last seen
--   the week before); its rates are then NULL.

with active as (
    select
        cast(date_trunc('week', crawl_date) as date) as period_start,
        listing_key,
        location_key,
        property_type_key,
        transaction_type,
        'ACTIVE' as kind,
        cast(null as double) as price_change_pct,
        cast(null as int) as days_on_market,
        row_number() over (
            partition by listing_key, cast(date_trunc('week', crawl_date) as date)
            order by crawl_timestamp desc, observation_id desc
        ) as period_rank
    from {{ ref('fct_listing_snapshot') }}
    where is_market_eligible
),

events as (
    select
        cast(date_trunc('week', event_at) as date) as period_start,
        listing_key,
        location_key,
        property_type_key,
        transaction_type,
        case
            when is_price_decrease then 'PRICE_CUT'
            when is_price_increase then 'PRICE_RISE'
            else event_type
        end as kind,
        price_change_pct,
        days_on_market
    from {{ ref('fct_listing_event') }}
    where is_market_eligible
      and not is_initial_load
      and event_type in ('NEW', 'REMOVED', 'PRICE_CHANGED')
),

period_rows as (
    select period_start, listing_key, location_key, property_type_key, transaction_type,
           kind, price_change_pct, days_on_market
    from active
    where period_rank = 1
    union all
    select *
    from events
),

located as (
    select
        r.*,
        loc.province,
        loc.area_zone,
        loc.district,
        pt.property_category,
        pt.property_type
    from period_rows r
    inner join {{ ref('dim_location') }} loc
        on r.location_key = loc.location_key
    inner join {{ ref('dim_property_type') }} pt
        on r.property_type_key = pt.property_type_key
),

aggregated as (
    select
        period_start,
        case
            when grouping(district) = 0 then 'DISTRICT'
            when grouping(area_zone) = 0 then 'ZONE'
            else 'CITY'
        end as geo_level,
        case
            when grouping(district) = 0 then district
            when grouping(area_zone) = 0 then area_zone
            else province
        end as geo_name,
        area_zone,
        transaction_type,
        property_category,
        property_type,

        count(distinct case when kind = 'ACTIVE' then listing_key end) as active_listing_count,
        count(distinct case when kind = 'NEW' then listing_key end) as new_listing_count,
        count(distinct case when kind = 'REMOVED' then listing_key end) as removed_listing_count,
        count(distinct case when kind = 'PRICE_CUT' then listing_key end) as price_cut_listing_count,
        count(distinct case when kind = 'PRICE_RISE' then listing_key end) as price_rise_listing_count,

        percentile_approx(case when kind = 'PRICE_CUT' then price_change_pct end, 0.5) as median_price_cut_pct,
        percentile_approx(case when kind = 'REMOVED' then days_on_market end, 0.5) as median_days_on_market_at_removal
    from located
    group by grouping sets (
        (period_start, transaction_type, property_category, property_type, province),
        (period_start, transaction_type, property_category, property_type, province, area_zone),
        (period_start, transaction_type, property_category, property_type, province, area_zone, district)
    )
)

select
    {{ date_key('period_start') }} as period_key,
    period_start,
    date_add(period_start, 6) as period_end,
    'week' as period_grain,
    geo_level,
    geo_name,
    area_zone,
    transaction_type,
    case transaction_type when 'sale' then 'Bán' when 'rent' then 'Cho thuê' end as transaction_type_label,
    property_category,
    property_type,

    active_listing_count,
    new_listing_count,
    removed_listing_count,
    new_listing_count - removed_listing_count as net_supply_change,
    price_cut_listing_count,
    price_rise_listing_count,

    case when active_listing_count > 0
        then removed_listing_count * 100.0 / active_listing_count end as removal_rate_pct,
    case when active_listing_count > 0
        then price_cut_listing_count * 100.0 / active_listing_count end as price_cut_rate_pct,
    median_price_cut_pct,
    median_days_on_market_at_removal,

    -- Rates from small samples are noisy; dashboards should filter on this.
    active_listing_count >= {{ var('trend_min_sample') }} as is_reliable
from aggregated
