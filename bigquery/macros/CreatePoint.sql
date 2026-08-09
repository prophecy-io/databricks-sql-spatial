{%- macro CreatePoint(relation, matchFields) -%}
    {%- set invalid_fields = [] -%}
    {%- for fields in matchFields %}
        {%- if fields[0] | length == 0 or fields[1] | length == 0 or fields[2] | length == 0 %}
            {%- do invalid_fields.append(true) %}
        {%- endif %}
    {%- endfor %}

    {# sanitize relation name (Jinja-safe, no python methods) #}
    {% set _rel_raw = relation | string | replace('`', '') | replace("'", '') %}
    {% set _rel = _rel_raw | trim %}
    {% if '.' in _rel %}
        {% set rel_ident = '`' ~ (_rel | replace('.', '`.`')) ~ '`' %}
    {% else %}
        {% set rel_ident = '`' ~ _rel ~ '`' %}
    {% endif %}

    {%- if matchFields | length == 0 or invalid_fields | length > 0 %}
        select * from {{ rel_ident }}
    {%- else %}
        select
            *,
            {%- for fields in matchFields %}
                CONCAT('POINT (', `{{ fields[0] }}`, ' ', `{{ fields[1] }}`, ')') as `{{ fields[2] }}`{% if not loop.last %},{% endif %}
            {%- endfor %}
        from {{ rel_ident }}
    {%- endif %}
{%- endmacro -%}