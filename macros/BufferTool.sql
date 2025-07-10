{%- macro BufferTool(table_name,schema,polygonColumnName,distance,unit) -%}
  {{ log("table_name=" ~ table_name, info=True) }}
  {{ log("schema=" ~ schema, info=True) }}
  {{ log("polygonColumnName=" ~ polygonColumnName, info=True) }}
  {{ log("distance=" ~ distance, info=True) }}
  {{ log("unit=" ~ unit, info=True) }}

  {%- if unit == 'kilometers' -%}
    {%- set distance_meters = distance * 1000 -%}
  {%- else -%}
    {%- set distance_meters = distance * 1609.34 -%}
  {%- endif -%}

  SELECT
    {{polygonColumnName}} as input,
    ST_AsText(
      ST_Transform(
        ST_Buffer(
          ST_Transform(
            ST_GeomFromText({{polygonColumnName}}, 4326),
            3857
          ),
          {{distance_meters}}
        ),
        4326
      )
    ) as output
  FROM
    {{table_name}}

{%- endmacro -%}
