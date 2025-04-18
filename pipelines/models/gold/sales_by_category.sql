SELECT 
    p.product_category,
    ROUND(SUM(oi.price), 2) AS total_revenue,
    COUNT(oi.order_id) AS total_quantity_sold,
    ROUND(AVG(oi.price), 2) AS average_price_per_item
FROM 
    delta_scan('../delta_lake/silver/order_items_silver') oi
JOIN 
    delta_scan('../delta_lake/silver/products_silver') p ON oi.product_id = p.product_id
GROUP BY 
    p.product_category;