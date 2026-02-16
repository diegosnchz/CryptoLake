{% macro spark__create_table_as(temporary, relation, compiled_code, language='sql') -%}
  {%- if relation.schema == 'staging' -%}
    {%- set bucket = 'cryptolake-silver' -%}
  {%- else -%}
    {%- set bucket = 'cryptolake-' ~ relation.schema -%}
  {%- endif -%}

  create or replace table {{ relation }}
  using iceberg
  location 's3://{{ bucket }}/{{ relation.identifier }}'
  as
  {{ compiled_code }}
{%- endmacro %}
