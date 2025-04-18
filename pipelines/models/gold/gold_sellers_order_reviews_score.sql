SELECT 
    s.seller_id,
    s.seller_city,
    s.seller_state,
    ROUND(AVG(orv.review_score), 2) AS avg_score_review
FROM 
    delta_scan('../delta_lake/silver/sellers_silver') s
JOIN 
    delta_scan('../delta_lake/silver/order_items_silver') oi ON s.seller_id = oi.seller_id
JOIN 
    delta_scan('../delta_lake/silver/order_reviews') orv ON oi.order_id = orv.order_id
GROUP BY 
    s.seller_id, s.seller_city, s.seller_state
HAVING 
    AVG(orv.review_score) < 3;