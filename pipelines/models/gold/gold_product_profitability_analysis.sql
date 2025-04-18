SELECT 
    p.product_id,
    p.product_category,
    ROUND(AVG(oi.price), 2) AS avg_price, -- preço médio
    ROUND(AVG(oi.freight_value), 2) AS avg_freight, -- média do frete do produto
    COUNT(oi.order_id) AS total_sales, -- total de vendas
    AVG(p.product_weight_g) AS avg_weight, -- média de peso do produto
    ROUND((AVG(oi.freight_value) / AVG(oi.price)) * 100, 2) AS freight_pct_price, -- percentual do frete em relação ao preço médio do produto
    CASE 
        WHEN (AVG(oi.freight_value) / AVG(oi.price)) * 100 >= 30 THEN True
        ELSE False
    END AS critical_product
FROM delta_scan('../delta_lake/silver/order_items_silver') oi
JOIN delta_scan('../delta_lake/silver/products_silver') p 
    ON oi.product_id = p.product_id
GROUP BY 
    p.product_id, 
    p.product_category;