{{ config(
    materialized='incremental',
    file_format='delta',
    unique_key='observation_id',
    incremental_strategy='merge',
    partition_by=['crawl_date'],
    merge_partition_column='crawl_date',
    on_schema_change='append_new_columns',
    schema='silver'
) }}

-- Valid observations with the previous observation of the same listing and
-- the resulting change_type. Grain: one valid observation.
--
-- Incremental: a run only classifies the observations staged since the last
-- run (the batch). The previous observation of each listing comes from the
-- last row this table already holds for it, so the history is never re-sorted.
--
-- A batch observation older than the last stored observation of its listing
-- (a crawl run loaded late or reloaded) changes the order of that listing's
-- history. Such listings are recomputed from their full staged history.
--
-- first_seen_at / observation_number / change_type are exact for every row.
-- last_seen_at is the last valid crawl of the listing as of the run that
-- wrote the row, so it is exact on the latest row (listing_current).
-- changes_loaded_at tells downstream incremental models which rows changed.

{% set order_columns = 'crawl_timestamp, snapshot_id, _ingested_at, observation_id' %}

{% if is_incremental() %}

{% set incremental_ctes %}
batch as (
    select *
    from {{ ref('stg_nhatot_properties') }}
    where is_valid_observation = true
      and {{ incremental_batch_predicate(ref('stg_nhatot_properties'), '_ingested_at') }}
),

batch_listings as (
    select listing_key, min(struct({{ order_columns }})) as first_batch_position
    from batch
    group by listing_key
),

-- Rows already stored for the listings of the batch, without the batch
-- observations themselves (a reloaded run overwrites its own rows).
stored as (
    select
        t.listing_key,
        t.observation_id,
        struct(t.crawl_timestamp, t.snapshot_id, t._ingested_at, t.observation_id) as position,
        t.price_vnd,
        t.content_hash,
        t.observation_number,
        t.first_seen_at
    from {{ this }} t
    left semi join batch_listings b
        on t.listing_key = b.listing_key
    left anti join batch n
        on t.observation_id = n.observation_id
),

stored_last as (
    select
        listing_key,
        max(position) as last_position,
        max_by(price_vnd, position) as price_vnd,
        max_by(content_hash, position) as content_hash,
        max(observation_number) as observation_number,
        min(first_seen_at) as first_seen_at
    from stored
    group by listing_key
),

-- Listings whose batch does not come strictly after their stored history.
late_listings as (
    select b.listing_key
    from batch_listings b
    inner join stored_last s
        on b.listing_key = s.listing_key
    where b.first_batch_position <= s.last_position
)
{% endset %}

{#- Reading the full staged history is only needed when some listing is late. -#}
{% set has_late_listings = execute and (
    run_query('with ' ~ incremental_ctes ~ ' select 1 from late_listings limit 1').rows | length > 0) %}

with
{{ incremental_ctes }},

-- Observations to (re)write: the batch, plus the full history of late listings.
source_rows as (
    select *
    from batch
    {% if has_late_listings %}

    union all

    select s.*
    from {{ ref('stg_nhatot_properties') }} s
    left semi join late_listings l
        on s.listing_key = l.listing_key
    left anti join batch b
        on s.observation_id = b.observation_id
    where s.is_valid_observation = true
    {% endif %}
),

-- The last stored row of every other listing seeds the window functions.
sequence_input as (
    select
        listing_key,
        observation_id,
        crawl_timestamp,
        snapshot_id,
        _ingested_at,
        price_vnd,
        content_hash,
        true as is_output,
        cast(null as int) as seed_observation_number,
        cast(null as timestamp) as seed_first_seen_at
    from source_rows

    union all

    select
        s.listing_key,
        s.last_position.observation_id,
        s.last_position.crawl_timestamp,
        s.last_position.snapshot_id,
        s.last_position._ingested_at,
        s.price_vnd,
        s.content_hash,
        false as is_output,
        s.observation_number,
        s.first_seen_at
    from stored_last s
    {% if has_late_listings %}
    left anti join late_listings l
        on s.listing_key = l.listing_key
    {% endif %}
),

{% else %}

with source_rows as (
    select *
    from {{ ref('stg_nhatot_properties') }}
    where is_valid_observation = true
),

sequence_input as (
    select
        listing_key,
        observation_id,
        crawl_timestamp,
        snapshot_id,
        _ingested_at,
        price_vnd,
        content_hash,
        true as is_output,
        cast(null as int) as seed_observation_number,
        cast(null as timestamp) as seed_first_seen_at
    from source_rows
),

{% endif %}

sequenced as (
    select
        observation_id,
        is_output,
        lag(price_vnd) over listing_order as previous_price_vnd,
        lag(content_hash) over listing_order as previous_content_hash,
        -- A seed row is first in its listing and carries its own number.
        coalesce(max(seed_observation_number) over listing_all, 0)
            + row_number() over listing_order
            - max(case when is_output then 0 else 1 end) over listing_all as observation_number,
        min(coalesce(seed_first_seen_at, crawl_timestamp)) over listing_all as first_seen_at,
        max(crawl_timestamp) over listing_all as last_seen_at
    from sequence_input
    window
        listing_order as (partition by listing_key order by {{ order_columns }}),
        listing_all as (partition by listing_key)
),

with_previous as (
    select
        r.*,
        q.previous_price_vnd,
        q.previous_content_hash,
        q.observation_number,
        q.first_seen_at,
        q.last_seen_at
    from source_rows r
    inner join sequenced q
        on r.observation_id = q.observation_id
    where q.is_output
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
    change_type as listing_status,
    current_timestamp() as changes_loaded_at
from classified
