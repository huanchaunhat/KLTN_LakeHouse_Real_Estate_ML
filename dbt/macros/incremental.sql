{#
  Helpers for incremental models.

  Load timestamps (_ingested_at, changes_loaded_at) cannot prune anything by
  themselves: Spark 3.3 writes timestamps as INT96, which has no Parquet
  min/max statistics, and they sit past the 32 columns Delta keeps stats for.
  So the batch is located through the crawl_date partitions instead: one cheap
  single-column query finds the partitions holding rows newer than the
  watermark, and the model then reads only those partitions.
#}

{#- SQL literal of max(column) in the current model, or of `default` on an empty table. -#}
{% macro incremental_watermark(column, default='1970-01-01 00:00:00') %}
    {%- if not execute -%}{{ return("timestamp'" ~ default ~ "'") }}{%- endif -%}
    {%- set result = run_query('select cast(max(' ~ column ~ ') as string) from ' ~ this) -%}
    {%- set value = result.columns[0].values()[0] -%}
    {{ return("timestamp'" ~ (value if value is not none else default) ~ "'") }}
{% endmacro %}

{#-
  Predicate selecting the rows of `relation` loaded since the last run of the
  current model: `column` newer than max(`watermark_column`) of the model,
  restricted to the `partition_column` values that hold such rows.
-#}
{% macro incremental_batch_predicate(relation, column, watermark_column=none, partition_column='crawl_date') %}
    {%- set watermark = incremental_watermark(watermark_column or column) -%}
    {%- set partitions = distinct_values_in_list(relation, partition_column, column ~ ' > ' ~ watermark) -%}
    {%- if partitions is none -%}{{ return('false') }}{%- endif -%}
    {{ return('cast(' ~ partition_column ~ ' as string) in ' ~ partitions ~ ' and ' ~ column ~ ' > ' ~ watermark) }}
{% endmacro %}

{#- Distinct values of `column` in `relation` matching `where`, as a SQL IN list ('a', 'b'), or none. -#}
{% macro distinct_values_in_list(relation, column, where='true') %}
    {%- if not execute -%}{{ return(none) }}{%- endif -%}
    {%- set result = run_query(
        'select distinct cast(' ~ column ~ ' as string) from ' ~ relation ~ ' where ' ~ where) -%}
    {%- set values = result.columns[0].values() | reject('none') | list -%}
    {%- if values | length == 0 -%}{{ return(none) }}{%- endif -%}
    {{ return("('" ~ values | join("', '") ~ "')") }}
{% endmacro %}

{#
  Override of dbt-spark 1.5 spark__get_merge_sql (same SQL), with one addition
  for models that set `merge_partition_column`: the ON clause is restricted to
  the target partitions present in the source, as literals, so Delta only
  scans and rewrites those partitions instead of the whole table. A row
  matches its target row only inside the same partition, so this does not
  change the result. (Delta 2.2 already materializes the merge source once.)
#}
{% macro spark__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) %}
  {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
  {%- set partition_column = config.get('merge_partition_column') -%}
  {%- if partition_column -%}
      {%- set partition_values = distinct_values_in_list(source, partition_column) -%}
      {%- if partition_values is not none -%}
          {% do predicates.append('cast(DBT_INTERNAL_DEST.' ~ partition_column ~ ' as string) in ' ~ partition_values) %}
      {%- endif -%}
  {%- endif -%}
  {%- set dest_columns = adapter.get_columns_in_relation(target) -%}
  {%- set merge_update_columns = config.get('merge_update_columns') -%}
  {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
  {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}

  {% if unique_key %}
      {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
          {% for key in unique_key %}
              {% set this_key_match %}
                  DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
              {% endset %}
              {% do predicates.append(this_key_match) %}
          {% endfor %}
      {% else %}
          {% set unique_key_match %}
              DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
          {% endset %}
          {% do predicates.append(unique_key_match) %}
      {% endif %}
  {% else %}
      {% do predicates.append('FALSE') %}
  {% endif %}

  {{ sql_header if sql_header is not none }}

  merge into {{ target }} as DBT_INTERNAL_DEST
      using {{ source }} as DBT_INTERNAL_SOURCE
      on {{ predicates | join(' and ') }}

      when matched then update set
        {% if update_columns -%}{%- for column_name in update_columns %}
            {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
        {%- else %} * {% endif %}

      when not matched then insert *
{% endmacro %}
