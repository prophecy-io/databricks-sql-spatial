{%- macro SpatialInfo_02(table_name, schema, polygonColumnName, centroid) -%}
  {{ log("table_name=" ~ table_name, info=True) }}
  {{ log("schema=" ~ schema, info=True) }}
  {{ log("polygonColumnName=" ~ polygonColumnName, info=True) }}
  {{ log("centroid=" ~ centroid, info=True) }}

  SELECT
    ST_AsText(ST_Centroid(ST_GeomFromText({{polygonColumnName}}))) as centroid2,
    {{polygonColumnName}} as input
  FROM
    {{table_name}}

{%- endmacro -%}
