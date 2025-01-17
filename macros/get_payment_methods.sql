{% macro get_payment_methods(table, column) %}

{% set payment_methods_query %}

  select distinct {{ column }}
  from {{ table }}
  order by 1

{% endset %}

{% set results = run_query(payment_methods_query) %}

{% if execute %}

  {# Return the first column #}
  {% set results_list = results.columns[0].values() %}

{% else %}

  {% set results_list = [] %}

{% endif %}

{{ return(results_list) }}

{% endmacro %}
