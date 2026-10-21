{{ config(
    materialized='incremental',
    file_format='delta',
    unique_key='snapshot_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    schema='silver'
) }}

-- Listing counts of each crawl run, read by int_crawl_snapshots.
-- Grain: one row per crawl run (snapshot).
--
-- Incremental: only the runs staged since the last run are recounted. Bronze
-- loads a crawl run as a whole, so the batch holds every row of those runs.

with observations as (
    select
        snapshot_id,
        listing_key,
        crawl_timestamp,
        _ingested_at,
        coalesce(http_status in (404, 410), false) as is_gone,
        is_valid_observation
    from {{ ref('stg_nhatot_properties') }}
    {% if is_incremental() %}
    where {{ incremental_batch_predicate(ref('stg_nhatot_properties'), '_ingested_at', 'last_ingested_at') }}
    {% endif %}
)

select
    snapshot_id,
    min(crawl_timestamp) as snapshot_started_at,
    max(crawl_timestamp) as snapshot_finished_at,
    count(distinct case when not is_gone then listing_key end) as online_listing_count,
    count(distinct case when is_valid_observation then listing_key end) as valid_listing_count,
    count(distinct case when is_gone then listing_key end) as gone_listing_count,
    max(_ingested_at) as last_ingested_at
from observations
group by snapshot_id
