SELECT 
    p.product_category_name,
    SUM(oi.price) AS total_revenue,
    COUNT(oi.order_id) AS total_quantity_sold,
    ROUND(AVG(oi.price), 2) AS average_price_per_item,
    ROUND(AVG(oi.freight_value), 2) AS average_freight,
    ROUND(AVG(pr.product_weight_g), 2) AS average_product_weight_per_category
FROM 
    delta_scan('../delta_lake/silver/order_items_silver') oi
JOIN 
    delta_scan('../delta_lake/silver/products_silver') pr ON oi.product_id = pr.product_id
JOIN 
    delta_scan('../delta_lake/bronze/product_category_name_translation_bronze') p ON pr.product_category = p.product_category_name
GROUP BY 
    p.product_category_name;