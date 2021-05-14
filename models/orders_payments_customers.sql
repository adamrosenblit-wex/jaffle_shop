with orders as (

    select {{ star_with_col_alias_prefix(from=ref('stg_orders'), prefix='orders__' , relation_alias='stg_orders', except=[]) }}
    from {{ ref('stg_orders') }} stg_orders

),

payments as (

    select {{ star_with_col_alias_prefix(from=ref('stg_payments'), prefix='payments__' , relation_alias='stg_payments', except=[]) }}
    from {{ ref('stg_payments') }} stg_payments

),

customers as (

    select {{ star_with_col_alias_prefix(from=ref('stg_customers'), prefix='customers__' , relation_alias='stg_customers', except=[]) }}
    from {{ ref('stg_customers') }} stg_customers

)

select
  {{ dbt_utils.surrogate_key(['orders__order_id', 'payments__payment_id', 'customers__customer_id']) }} as orders_payments_customers_surrogate_key,
  *

from orders

  join payments
    on payments.payments__order_id = orders.orders__order_id

  join customers
    on customers.customers__customer_id = orders.orders__customer_id
