{#
  By default dbt would write models into "<target_schema>_<custom_schema>"
  (e.g. warehouse_app_staging). Our warehouse-db already has exactly the
  schemas we want (staging, intermediate, marts, snapshots - created in
  Stage 2/5's Postgres init scripts), so this override makes dbt use the
  custom schema name exactly as given in dbt_project.yml, with no prefix.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
