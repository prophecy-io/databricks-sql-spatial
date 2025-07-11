{%- macro SpatialInfo(table_name,schema,polygonColumnName,distance,unit) -%}
  {{ log("table_name=" ~ table_name, info=True) }}
  {{ log("schema=" ~ schema, info=True) }}
  {{ log("polygonColumnName=" ~ polygonColumnName, info=True) }}
  {{ log("distance=" ~ distance, info=True) }}
  {{ log("unit=" ~ unit, info=True) }}

  SELECT
    {{polygonColumnName}} as input
  FROM
    {{table_name}}

{%- endmacro -%}
