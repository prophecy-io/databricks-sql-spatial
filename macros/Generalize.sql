{%- macro Generalize(table_name,schema,polygonColumnName,threshold,unit) -%}
    {{ log("table_name=" ~ table_name, info=True) }}
    {{ log("schema=" ~ schema, info=True) }}
    {{ log("polygonColumnName=" ~ polygonColumnName, info=True) }}
    {{ log("threshold=" ~ threshold, info=True) }}
    {{ log("unit=" ~ unit, info=True) }}

    select
      {{polygonColumnName}} as input,
      andre_dev.alteryx_spatial.generalize(
        {{polygonColumnName}}, 
        {{threshold}}, 
        True
      ) as output
      from 
        {{ table_name }}
{%- endmacro -%}

