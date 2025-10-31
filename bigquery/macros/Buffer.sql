{%- macro Buffer(table_name, schema, geom_column_name, distance, unit) -%}
  {{ log("table_name=" ~ table_name, info=True) }}
  {{ log("schema=" ~ schema, info=True) }}
  {{ log("geom_column_name=" ~ geom_column_name, info=True) }}
  {{ log("distance=" ~ distance, info=True) }}
  {{ log("unit=" ~ unit, info=True) }}

  {%- if unit == 'kms' or unit == 'kilometers' -%}
    {%- set distance_meters = distance * 1000 -%}
  {%- else -%}
    {%- set distance_meters = distance * 1609.34 -%}
  {%- endif -%}

  {# 
    BigQuery approach is simpler than Databricks:
    - Uses native geography type for geodesic calculations
    - No coordinate transformations needed
    - ST_BUFFER automatically handles geodesic buffering on Earth's surface
  #}
  {# sanitize identifiers in case they were passed as quoted strings #}
  {# take only the first relation if a comma-separated list is passed #}
  {% set _tbl_raw = (table_name | replace('`', '') | replace("'", '')) %}
  {% set _tbl = (_tbl_raw | trim) %}
  {% set _col = (geom_column_name | replace('`', '') | replace("'", '')) %}

  {# determine safe column identifier #}
  {% set col_ident = _col %}
  {% if ('.' not in _col) and (_col | length > 0) and (_col[0] != '`') %}
    {% set col_ident = '`' ~ _col ~ '`' %}
  {% endif %}

  {# determine safe table identifier #}
  {% set tbl_ident = _tbl %}
  {% if (_tbl | length > 0) and (_tbl[0] != '`') %}
    {% set tbl_ident = '`' ~ _tbl ~ '`' %}
  {% endif %}

  SELECT
    {{ col_ident }} as input,
    ST_ASGEOJSON(
      ST_BUFFER(
        ST_GEOGFROMTEXT({{ col_ident }}),
        {{ distance_meters }}
      )
    ) as output
  FROM
    {{ tbl_ident }}

{%- endmacro -%}

