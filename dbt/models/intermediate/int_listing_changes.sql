{{ config(
    materialized='table',
    file_format='delta',
    schema='silver'
) }}

with valid_observations as (
    select *
    from {{ ref('stg_nhatot_properties') }}
    where is_valid_observation = true
),

with_previous as (
    select
        *,
        lag(price_vnd) over (
            partition by listing_key
            order by crawl_timestamp, snapshot_id, _ingested_at, observation_id
        ) as previous_price_vnd,
        lag(content_hash) over (
            partition by listing_key
            order by crawl_timestamp, snapshot_id, _ingested_at, observation_id
        ) as previous_content_hash,
        row_number() over (
            partition by listing_key
            order by crawl_timestamp, snapshot_id, _ingested_at, observation_id
        ) as observation_number,
        min(crawl_timestamp) over (
            partition by listing_key
        ) as first_seen_at,
        max(crawl_timestamp) over (
            partition by listing_key
        ) as last_seen_at
    from valid_observations
),

classified as (
    select
        *,
        case
            when observation_number = 1 then 'NEW'
            when not (price_vnd <=> previous_price_vnd) then 'PRICE_CHANGED'
            when not (content_hash <=> previous_content_hash) then 'UPDATED'
            else 'UNCHANGED'
        end as change_type,
        case
            when previous_price_vnd is not null and price_vnd is not null
                then price_vnd - previous_price_vnd
        end as price_change_vnd,
        case
            when previous_price_vnd > 0 and price_vnd is not null
                then (
                    cast(price_vnd as double) - cast(previous_price_vnd as double)
                ) / cast(previous_price_vnd as double) * 100.0
        end as price_change_pct
    from with_previous
)

select
    *,
    change_type as listing_status
from classified
