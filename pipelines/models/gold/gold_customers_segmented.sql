WITH customer_orders AS (
    SELECT
        o.customer_id,
        o.order_id,
        o.order_purchase_timestamp
    FROM delta_scan('../delta_lake/silver/orders_full_data_silver') o
),

payments_per_order AS (
    SELECT
        op.order_id,
        SUM(op.payment_value) AS total_payment
    FROM delta_scan('../delta_lake/silver/payments_silver') op
    GROUP BY op.order_id
),

orders_with_payment AS (
    SELECT
        co.customer_id,
        co.order_id,
        co.order_purchase_timestamp,
        ppo.total_payment
    FROM customer_orders co
    LEFT JOIN payments_per_order ppo ON co.order_id = ppo.order_id
),

agg_metrics_per_customer AS (
    SELECT
        customer_id,
        COUNT(order_id) AS total_orders,
        SUM(total_payment) AS total_spent,
        AVG(total_payment) AS avg_ticket,
        MAX(order_purchase_timestamp) AS last_order_date,
        MIN(order_purchase_timestamp) AS first_order_date
    FROM orders_with_payment
    GROUP BY customer_id
)

SELECT
    ac.customer_id,
    ac.customer_cep AS customer_cep,
    ac.avg_latitude,
    ac.avg_longitude,
    amc.total_orders,
    amc.total_spent,
    amc.avg_ticket,
    amc.first_order_date,
    amc.last_order_date
FROM delta_scan('../delta_lake/silver/aggregated_customers') ac
LEFT JOIN agg_metrics_per_customer amc
    ON ac.customer_id = amc.customer_id;