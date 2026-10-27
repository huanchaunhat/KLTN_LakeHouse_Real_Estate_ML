{# One-time migration for observations staged before list_time was used as
   the fallback for orig_list_time. Recover real source dates, without
   reclassifying observations or changing their business values. #}
{% macro backfill_legacy_posted_at() %}
    {% if not execute %}{{ return(none) }}{% endif %}

    {# Historical rows include large descriptions. Keep shuffle tasks small
       for the 1 GB executors; AQE must not merge them back into large tasks.
       These settings apply only to this run-operation's Thrift session. #}
    {% do run_query('set spark.sql.shuffle.partitions=96') %}
    {% do run_query('set spark.sql.adaptive.coalescePartitions.enabled=false') %}

    {% set stage = ref('stg_nhatot_properties') %}
    {% set bronze = source('raw_lakehouse', 'raw_properties') %}
    {% set stage_sql %}
        merge into {{ stage }} as t
        using (
            select
                source,
                cast(source_listing_id as string) as source_listing_id,
                snapshot_id,
                to_timestamp(from_unixtime(cast(
                    coalesce(payload.ad.orig_list_time, payload.ad.list_time) / 1000 as bigint
                ))) as posted_at
            from {{ bronze }}
            where coalesce(payload.ad.orig_list_time, payload.ad.list_time) is not null
        ) as s
        on t.source = s.source
           and t.source_listing_id = s.source_listing_id
           and t.snapshot_id = s.snapshot_id
        when matched and t.posted_at is null and s.posted_at is not null
        then update set posted_at = s.posted_at
    {% endset %}
    {% do run_query(stage_sql) %}
    {% do log('Recovered missing source post dates in ' ~ stage, info=true) %}

    {# Update history first, then current using its existing observation_id.
       Re-ranking only repaired history rows could select an older observation. #}
    {% for model in ['int_listing_changes', 'listing_current'] %}
        {% set target = ref(model) %}
        {% set repair_sql %}
            merge into {{ target }} as t
            using (
                select observation_id, posted_at
                from {{ stage }}
                where posted_at is not null
            ) as s
            on t.observation_id = s.observation_id
            when matched and t.posted_at is null
            then update set
                posted_at = s.posted_at,
                changes_loaded_at = current_timestamp()
        {% endset %}
        {% do run_query(repair_sql) %}
        {% do log('Recovered missing source post dates in ' ~ target, info=true) %}
    {% endfor %}
{% endmacro %}
