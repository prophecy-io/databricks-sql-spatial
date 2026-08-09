{%- macro Simplify(table_name, schema, geom_column_name, tolerance, unit) -%}
  {{ log("table_name=" ~ table_name, info=True) }}
  {{ log("schema=" ~ schema, info=True) }}
  {{ log("geom_column_name=" ~ geom_column_name, info=True) }}
  {{ log("tolerance=" ~ tolerance, info=True) }}
  {{ log("unit=" ~ unit, info=True) }}

  {%- if unit == 'kms' or unit == 'kilometers' -%}
    {%- set tolerance_meters = tolerance * 1000 -%}
  {%- else -%}
    {%- set tolerance_meters = tolerance * 1609.34 -%}
  {%- endif -%}

  {# 
    BigQuery approach is simpler than Databricks:
    - Uses native geography type for geodesic calculations
    - No coordinate transformations needed
    - ST_SIMPLIFY automatically handles geodesic simplification on Earth's surface
  #}
  SELECT
    `{{ geom_column_name }}` as input,
    ST_ASGEOJSON(ST_SIMPLIFY(ST_GEOGFROMTEXT(`{{ geom_column_name }}`), {{ tolerance_meters }})) as output
  FROM
    {# Build a safe fully-qualified identifier. Accepts:
       - schema as dataset or project.dataset (or project:dataset)
       - table_name as bare or already qualified
     #}
    {%- set _schema_raw = (schema | string | replace('`', '') | replace("'", '') | replace(':', '.') | trim) -%}
    {%- set _schema = _schema_raw -%}
    {# If Prophecy passes a structure like "[{name: dataset}]", fall back to target.schema #}
    {%- if '[' in _schema_raw or '{' in _schema_raw -%}
      {%- set _schema = target.schema -%}
    {%- endif -%}

    {%- set _table  = (table_name | string | replace('`', '') | replace("'", '') | trim) -%}
    {%- if _table | length == 0 -%}
      {{ exceptions.raise('Simplify: table_name must be provided') }}
    {%- endif -%}
    {# default schema to current target if not provided #}
    {%- if (_schema | length) == 0 and ('.' not in _table) -%}
      {%- set _schema = target.schema -%}
    {%- endif -%}
    {%- set _project = target.project -%}

    {%- if '.' in _table -%}
      {%- set _fq = _table -%}
    {%- elif _schema | length > 0 -%}
      {%- if '.' in _schema -%}
        {%- set _fq = _schema ~ '.' ~ _table -%}
      {%- else -%}
        {%- set _fq = _project ~ '.' ~ _schema ~ '.' ~ _table -%}
      {%- endif -%}
    {%- else -%}
      {%- set _fq = _project ~ '.' ~ target.schema ~ '.' ~ _table -%}
    {%- endif -%}
    `{{ _fq }}`

{%- endmacro -%}

