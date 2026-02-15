{%- macro Simplify(table_name, schema, geom_column_name, output_column_name, tolerance, unit) -%}
  {{ log("table_name=" ~ table_name, info=True) }}
  {{ log("schema=" ~ schema, info=True) }}
  {{ log("geom_column_name=" ~ geom_column_name, info=True) }}
  {{ log("output_column_name=" ~ output_column_name, info=True) }}
  {{ log("tolerance=" ~ tolerance, info=True) }}
  {{ log("unit=" ~ unit, info=True) }}

 {%- if unit == 'miles' -%}
    {%- set tolerance_meters = tolerance * 1609.34 -%}
  {%- elif unit == 'kilometers' -%}
      {%- set tolerance_meters = tolerance * 1000 -%}
  {%- else -%}
    {%- set tolerance_meters = tolerance -%}
  {%- endif -%}

  SELECT
    *,
    ST_AsText(
      ST_Transform(
        ST_Simplify(
          ST_Transform(
            ST_GeomFromText({{geom_column_name}}, 4326),
            3857
          ),
          {{tolerance_meters}}
        ),
        4326
      )
    ) as {{output_column_name}}
  FROM
    {{table_name}}

{%- endmacro -%}
