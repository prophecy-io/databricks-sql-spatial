{%- macro Generalize(table_name, schema, polygonColumnName, threshold, unit) -%}
    {{ log("table_name=" ~ table_name, info=True) }}
    {{ log("schema=" ~ schema, info=True) }}
    {{ log("polygonColumnName=" ~ polygonColumnName, info=True) }}
    {{ log("threshold=" ~ threshold, info=True) }}
    {{ log("unit=" ~ unit, info=True) }}

    SELECT 
      {{polygonColumnName}} as input,
      ST_AsText(
          ST_Simplify(
            ST_GeomFromText({{polygonColumnName}}), 
            {{threshold}}
          ) 
      ) as output
    FROM 
      {{table_name}}
{%- endmacro -%}
