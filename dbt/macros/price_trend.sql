{#
  Price-trend aggregate at a given period grain ('week' or 'month').

  Grain: period x geo (city / zone / district) x transaction_type x property_type.

  - Each listing is counted once per period, with its last observation in
    that period, so listing counts and medians are not inflated by how many
    times a listing was crawled.
  - Medians are computed from listing-level data for every geo level
    (GROUPING SETS), never by averaging medians of a lower level.
  - Price/m2 is compared within the same property_type to limit mix shift.
  - Outliers and non-market listings are excluded.
#}

{% macro price_trend(period) %}

with observations as (
    select
        *,
        cast(date_trunc('{{ period }}', crawl_date) as date) as period_start
    from {{ ref('fct_listing_snapshot') }}
    where is_market_eligible
      and not is_price_outlier
),

ranked as (
    select
        *,
        max(cast(is_price_decrease as int)) over (
            partition by listing_key, period_start
        ) as had_price_cut,
        max(cast(is_price_increase as int)) over (
            partition by listing_key, period_start
        ) as had_price_rise,
        row_number() over (
            partition by listing_key, period_start
            order by crawl_timestamp desc, observation_id desc
        ) as period_rank
    from observations
),

period_listings as (
    select
        r.period_start,
        r.transaction_type,
        r.price_vnd,
        r.price_per_m2,
        r.area_m2,
        r.had_price_cut,
        r.had_price_rise,
        cast(date_trunc('{{ period }}', fl.first_seen_at) as date) = r.period_start as is_new_in_period,
        loc.province,
        loc.area_zone,
        loc.district,
        pt.property_category,
        pt.property_type
    from ranked r
    inner join {{ ref('dim_location') }} loc
        on r.location_key = loc.location_key
    inner join {{ ref('dim_property_type') }} pt
        on r.property_type_key = pt.property_type_key
    inner join {{ ref('fct_listing') }} fl
        on r.listing_key = fl.listing_key
    where r.period_rank = 1
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

        count(*) as listing_count,
        sum(case when is_new_in_period then 1 else 0 end) as new_listing_count,
        sum(had_price_cut) as price_cut_count,
        sum(had_price_rise) as price_rise_count,

        percentile_approx(price_per_m2, 0.25) as p25_price_per_m2,
        percentile_approx(price_per_m2, 0.5) as median_price_per_m2,
        percentile_approx(price_per_m2, 0.75) as p75_price_per_m2,
        avg(price_per_m2) as avg_price_per_m2,
        percentile_approx(price_vnd, 0.5) as median_price_vnd,
        percentile_approx(area_m2, 0.5) as median_area_m2
    from period_listings
    group by grouping sets (
        (period_start, transaction_type, property_category, property_type, province),
        (period_start, transaction_type, property_category, property_type, province, area_zone),
        (period_start, transaction_type, property_category, property_type, province, area_zone, district)
    )
),

with_previous as (
    select
        *,
        lag(median_price_per_m2) over (
            partition by geo_level, geo_name, transaction_type, property_type
            order by period_start
        ) as prev_median_price_per_m2,
        lag(period_start) over (
            partition by geo_level, geo_name, transaction_type, property_type
            order by period_start
        ) as prev_period_start
    from aggregated
)

select
    {{ date_key('period_start') }} as period_key,
    period_start,
    {% if period == 'week' %}date_add(period_start, 6){% else %}last_day(period_start){% endif %} as period_end,
    '{{ period }}' as period_grain,
    geo_level,
    geo_name,
    area_zone,
    transaction_type,
    case transaction_type when 'sale' then 'Bán' when 'rent' then 'Cho thuê' end as transaction_type_label,
    property_category,
    property_type,

    listing_count,
    new_listing_count,
    price_cut_count,
    price_rise_count,
    p25_price_per_m2,
    median_price_per_m2,
    p75_price_per_m2,
    avg_price_per_m2,
    median_price_vnd,
    median_area_m2,

    -- Change vs the previous period of the same series, only when that period
    -- is the immediately preceding one.
    case
        when prev_period_start = {% if period == 'week' %}date_sub(period_start, 7){% else %}add_months(period_start, -1){% endif %}
         and prev_median_price_per_m2 > 0
            then (median_price_per_m2 - prev_median_price_per_m2) / prev_median_price_per_m2 * 100.0
    end as median_price_per_m2_change_pct,

    -- Medians from small samples are noisy; dashboards should filter on this.
    listing_count >= {{ var('trend_min_sample') }} as is_reliable
from with_previous

{% endmacro %}
