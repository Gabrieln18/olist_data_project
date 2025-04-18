SELECT 
    c.customer_city,
    c.customer_state,
    SUM(oi.price) AS total_revenue,
    COUNT(o.order_id) AS total_orders
FROM 
    delta_scan('../delta_lake/silver/orders_full_data_silver') o
JOIN 
    delta_scan('../delta_lake/silver/order_items_silver') oi ON o.order_id = oi.order_id
JOIN 
    delta_scan('../delta_lake/silver/customers_silver') c ON o.customer_id = c.customer_id
GROUP BY 
    c.customer_city, c.customer_state;