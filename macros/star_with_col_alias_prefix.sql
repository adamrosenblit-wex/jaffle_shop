{% macro star_with_col_alias_prefix(from, prefix, relation_alias=False, except=[]) -%}

  {%- do dbt_utils._is_relation(from, 'star_with_col_alias_prefix') -%}

  {#-- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. #}
  {%- if not execute -%}
      {{ return('') }}
  {% endif %}

  {%- set include_cols = [] %}
  {%- set cols = adapter.get_columns_in_relation(from) -%}
  {%- for col in cols -%}

      {%- if col.column not in except -%}
          {% do include_cols.append(col.column) %}

      {%- endif %}
  {%- endfor %}

  {%- for col in include_cols %}

      {% if relation_alias %}{{ relation_alias }}.{% else %}{%- endif -%}{{ col|trim }} as {{ prefix|trim|lower }}{{ col|trim|lower }}
      {%- if not loop.last %},{{ '\n  ' }}{% endif %}

  {%- endfor -%}
{%- endmacro %}
