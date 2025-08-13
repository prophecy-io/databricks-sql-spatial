{%- macro Buffer(table_name, schema, geom_column_name, output_column_name, distance, unit) -%}
  {{ log("table_name=" ~ table_name, info=True) }}
  {{ log("schema=" ~ schema, info=True) }}
  {{ log("geom_column_name=" ~ geom_column_name, info=True) }}
  {{ log("output_column_name=" ~ output_column_name, info=True) }}
  {{ log("distance=" ~ distance, info=True) }}
  {{ log("unit=" ~ unit, info=True) }}

 {%- if unit == 'kilometers' -%}
    {%- set distance_meters = distance * 1000 -%}
  {%- elif unit == 'miles' -%}
      {%- set distance_meters = distance * 1609.34 -%}
  {%- else -%}
    {%- set distance_meters = distance -%}
  {%- endif -%}

  SELECT
    *,
    ST_AsText(
      ST_Transform(
        ST_Buffer(
          ST_Transform(
            ST_GeomFromText({{geom_column_name}}, 4326),
            3857
          ),
          {{distance_meters}}
        ),
        4326
      )
    ) as {{output_column_name}}
  FROM
    {{table_name}}

{%- endmacro -%}
