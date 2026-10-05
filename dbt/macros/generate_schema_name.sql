{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if 'staging' in node.fqn
        or 'intermediate' in node.fqn
        or 'silver' in node.fqn -%}
        silver

    {%- elif 'gold' in node.fqn -%}
        gold

    {%- elif custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}

    {%- else -%}
        {{ target.schema }}

    {%- endif -%}

{%- endmacro %}
