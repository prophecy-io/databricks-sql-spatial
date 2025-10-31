{% macro SpatialMatch(
    relation_names,
    schemas,
    source_col,
    target_col,
    type
) -%}

  {% set fn_map = {
    'intersects': 'ST_INTERSECTS',
    'contains': 'ST_CONTAINS',
    'within': 'ST_WITHIN',
    'touches': 'ST_TOUCHES'
  } %}

  {% set source_relation = relation_names[0] %}
  {% set target_relation = relation_names[1] %}

  {% set source_columns = schemas[0] %}
  {% set target_columns = schemas[1] %}

  {% set source_select = [] %}
  {% for col in source_columns %}
    {% do source_select.append('source.' ~ col) %}
  {% endfor %}

  {% set target_select = [] %}
  {% for col in target_columns %}
    {% do target_select.append('target.' ~ col ~ ' AS target_' ~ col) %}
  {% endfor %}

  {% set spatial_fn = fn_map.get(type) %}

  SELECT
    {{ (source_select + target_select) | join(',\n    ') }}
  FROM {{ source_relation }} AS source
  CROSS JOIN {{ target_relation }} AS target
  WHERE
    {% if spatial_fn %}
      {{ spatial_fn }}(
        ST_GEOGFROMTEXT(source.{{ source_col }}),
        ST_GEOGFROMTEXT(target.{{ target_col }})
      )
    {% elif type == 'touches_or_intersects' %}
      ST_TOUCHES(
        ST_GEOGFROMTEXT(source.{{ source_col }}),
        ST_GEOGFROMTEXT(target.{{ target_col }})
      )
      OR ST_INTERSECTS(
        ST_GEOGFROMTEXT(source.{{ source_col }}),
        ST_GEOGFROMTEXT(target.{{ target_col }})
      )
    {% elif type == 'envelope' %}
      ST_INTERSECTS(
        ST_ENVELOPE(ST_GEOGFROMTEXT(source.{{ source_col }})),
        ST_ENVELOPE(ST_GEOGFROMTEXT(target.{{ target_col }}))
      )
    {% else %}
      1=1 -- fallback if no known type
    {% endif %}

{%- endmacro %}

