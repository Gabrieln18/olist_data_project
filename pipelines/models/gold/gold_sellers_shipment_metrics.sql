SELECT 
    oi.seller_id,
    s.seller_city,
    s.seller_state,
    COUNT(o.order_id) AS total_orders,
    SUM(CASE WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1 ELSE 0 END) AS delayed_orders,
    ROUND(SUM(CASE WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1 ELSE 0 END) * 100.0 / COUNT(o.order_id), 2) AS delayed_rate_percent,
    ROUND(AVG(CASE WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN DATE_DIFF('day', o.order_estimated_delivery_date, o.order_delivered_customer_date) ELSE 0 END), 2) AS avg_delay_days,
    MAX(CASE WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN o.order_delivered_customer_date ELSE NULL END) AS last_order_delayed
FROM 
    delta_scan('../delta_lake/silver/orders_full_data_silver') o
JOIN 
    delta_scan('../delta_lake/silver/order_items_silver') oi ON o.order_id = oi.order_id
JOIN 
    delta_scan('../delta_lake/silver/sellers_silver') s ON oi.seller_id = s.seller_id
GROUP BY 
    oi.seller_id, s.seller_city, s.seller_state
HAVING 
    SUM(CASE WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1 ELSE 0 END) > 0