{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}

with orders as (
    select * from {{ ref('stg_jaffle_shop__orders') }}
),

payments as (
    select * from {{ ref('stg_stripe__payments') }}
),

order_payments as (
    select
        order_id,
        {% for payment_method in payment_methods -%}
        sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount
        {%- if not loop.last %}, {% endif %}
        {% endfor %}
    from payments
    group by order_id
),

final as (
    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        {% for payment_method in payment_methods -%}
        coalesce(order_payments.{{ payment_method }}_amount, 0) as {{ payment_method }}_amount
        {%- if not loop.last %}, {% endif %}
        {% endfor %}
    from orders
    left join order_payments using (order_id)
)

select * from final
