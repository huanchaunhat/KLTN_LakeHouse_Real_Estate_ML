{#
  Weekly Delta Lake housekeeping, run by the Airflow DAG:
    dbt run-operation delta_maintenance

  Silver and Gold tables are rewritten on every run, and Delta keeps the files
  of old versions until VACUUM removes them, so MinIO would otherwise grow
  without bound. Bronze is append/merge only: OPTIMIZE compacts the small
  files that daily MERGEs leave behind.

  retain_hours keeps that much time travel for rollback (RESTORE TABLE ...
  TO VERSION AS OF ...). Delta refuses less than 168 hours by default.
#}

{% macro delta_maintenance(retain_hours=168) %}
    {% if not execute %}{{ return(none) }}{% endif %}

    {% set bronze = 'bronze.raw_properties' %}
    {% do log('OPTIMIZE ' ~ bronze, info=true) %}
    {% do run_query('OPTIMIZE ' ~ bronze) %}
    {% do log('VACUUM ' ~ bronze, info=true) %}
    {% do run_query('VACUUM ' ~ bronze ~ ' RETAIN ' ~ retain_hours ~ ' HOURS') %}

    {% for node in graph.nodes.values()
        if node.resource_type == 'model'
        and node.package_name == project_name
        and node.config.materialized in ('table', 'incremental') %}
        {% set relation = adapter.get_relation(
            database=node.database, schema=node.schema, identifier=node.alias) %}
        {% if relation is not none %}
            {% do log('VACUUM ' ~ relation, info=true) %}
            {% do run_query('VACUUM ' ~ relation ~ ' RETAIN ' ~ retain_hours ~ ' HOURS') %}
        {% endif %}
    {% endfor %}
{% endmacro %}
