{#
  Surrogate keys for Gold dimensions.
  Dimensions and facts must call the same macro so that the keys always join.
  xxhash64 returns BIGINT, which is cheaper to join and friendlier to BI tools
  than a sha2 string. NULL attributes map to an explicit "unknown" member.
#}

{% macro location_key(province_id, district_id, ward_id) -%}
    xxhash64(
        coalesce(cast({{ province_id }} as bigint), -1),
        coalesce(cast({{ district_id }} as bigint), -1),
        coalesce(cast({{ ward_id }} as bigint), -1)
    )
{%- endmacro %}

{% macro property_type_key(property_type, property_subtype) -%}
    xxhash64(
        coalesce({{ property_type }}, '__UNKNOWN__'),
        coalesce({{ property_subtype }}, '__NONE__')
    )
{%- endmacro %}

{% macro legal_status_key(legal_status) -%}
    xxhash64(coalesce({{ legal_status }}, '__UNKNOWN__'))
{%- endmacro %}

{% macro project_key(project_name) -%}
    xxhash64(coalesce({{ project_name }}, '__NONE__'))
{%- endmacro %}

{% macro date_key(date_expr) -%}
    cast(date_format({{ date_expr }}, 'yyyyMMdd') as int)
{%- endmacro %}
