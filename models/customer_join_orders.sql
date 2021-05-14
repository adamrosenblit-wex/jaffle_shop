with orders as (

    select {{ dbt_utils.star(from=ref('orders'), except=[]) }}
    from {{ ref('stg_orders') }}

),

customers as (

    select * from {{ ref('customers') }}

),
