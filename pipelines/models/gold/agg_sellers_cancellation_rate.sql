WITH pedidos_por_vendedor AS (
    SELECT 
        oi.seller_id,
        o.order_id,
        o.order_status
    FROM delta_scan('../delta_lake/silver/order_items_silver') oi
    JOIN delta_scan('../delta_lake/silver/orders_full_data_silver') o ON o.order_id = oi.order_id
),
reviews_por_vendedor AS (
    SELECT 
        oi.seller_id,
        r.review_score
    FROM delta_scan('../delta_lake/silver/order_items_silver') oi
    JOIN delta_scan('../delta_lake/silver/order_reviews') r ON r.order_id = oi.order_id
),
agg_cancellation AS (
    SELECT 
        seller_id,
        COUNT(*) AS total_orders,
        SUM(CASE WHEN order_status = 'canceled' THEN 1 ELSE 0 END) AS canceled_orders,
        ROUND(SUM(CASE WHEN order_status = 'canceled' THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) AS cancellation_rate
    FROM pedidos_por_vendedor
    GROUP BY seller_id
),
agg_reviews AS (
    SELECT 
        seller_id,
        COUNT(*) AS total_reviews,
        SUM(CASE WHEN review_score IN (1, 2) THEN 1 ELSE 0 END) AS negative_reviews,
        ROUND(SUM(CASE WHEN review_score IN (1, 2) THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) AS negative_review_rate
    FROM reviews_por_vendedor
    GROUP BY seller_id
)

SELECT 
    c.seller_id,
    c.total_orders,
    c.canceled_orders,
    c.cancellation_rate,
    r.total_reviews,
    r.negative_reviews,
    r.negative_review_rate
FROM agg_cancellation c
LEFT JOIN agg_reviews r ON c.seller_id = r.seller_id
ORDER BY cancellation_rate DESC, negative_review_rate DESC;