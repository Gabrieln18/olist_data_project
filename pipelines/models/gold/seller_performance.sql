SELECT
    s.seller_id,
    SUM(oi.price) AS total_revenue,
    COUNT(oi.order_id) AS total_sales
FROM
    delta_scan('../delta_lake/silver/order_items_silver') oi
JOIN 
    delta_scan('../delta_lake/silver/sellers_silver') s ON oi.seller_id = s.seller_id
GROUP BY
    s.seller_id;