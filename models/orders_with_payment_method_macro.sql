{% set payment_methods = get_payment_methods(table=ref('raw_payments'), column='payment_method') %}

with orders as (

    select {{ dbt_utils.star(from=ref('stg_orders'), except=[]) }}
    from {{ ref('stg_orders') }}

),

payments as (

    select {{ dbt_utils.star(from=ref('stg_payments'), except=[]) }}
    from {{ ref('stg_payments') }}

),

order_payments as (

    select
        order_id,

        {% for payment_method in payment_methods -%}
        sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount,
        {% endfor -%}

        sum(amount) as total_amount

    from payments

    {{ dbt_utils.group_by(n=1) }} --> group by 1

),

final as (

    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        orders.status,

        {% for payment_method in payment_methods -%}

        order_payments.{{ payment_method }}_amount,

        {% endfor -%}

        order_payments.total_amount as amount

    from orders

    left join order_payments using (order_id)

)

select * from final
