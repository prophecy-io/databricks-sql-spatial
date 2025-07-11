{%- macro SpatialInfo(table_name, schema, polygonColumnName, area) -%}
  {{ log("table_name=" ~ table_name, info=True) }}
  {{ log("schema=" ~ schema, info=True) }}
  {{ log("polygonColumnName=" ~ polygonColumnName, info=True) }}
  {{ log("area=" ~ arexa, info=True) }}

  SELECT
    ST_AsText(ST_Centroid(ST_GeomFromText({{polygonColumnName}}))) as centroid,
    {{polygonColumnName}} as input
  FROM
    {{table_name}}
  
{%- endmacro -%}
