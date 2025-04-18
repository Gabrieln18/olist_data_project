SELECT
    ac.customer_cep,
    ac.customer_city,
    ac.customer_state,
    ac.avg_latitude,
    ac.avg_longitude,
    COUNT(DISTINCT ac.customer_unique_id) AS total_customers,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT oi.seller_id) AS distinct_sellers,
    SUM(p.payment_value) AS total_sales_value,
    CASE 
        WHEN COUNT(DISTINCT oi.seller_id) = 0 THEN NULL
        ELSE ROUND(
            CAST(COUNT(DISTINCT o.order_id) AS FLOAT) / COUNT(DISTINCT oi.seller_id),
            2
        )
    END AS demand_supply_ratio
FROM delta_scan('../delta_lake/silver/aggregated_customers') ac
LEFT JOIN delta_scan('../delta_lake/silver/orders_full_data_silver') o ON ac.customer_id = o.customer_id
LEFT JOIN delta_scan('../delta_lake/silver/order_items_silver') oi ON o.order_id = oi.order_id
LEFT JOIN delta_scan('../delta_lake/silver/payments_silver') p ON o.order_id = p.order_id
GROUP BY
    ac.customer_cep,
    ac.customer_city,
    ac.customer_state,
    ac.avg_latitude,
    ac.avg_longitude
ORDER BY total_orders DESC;