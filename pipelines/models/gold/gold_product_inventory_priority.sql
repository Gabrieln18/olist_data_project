SELECT 
  p.product_id,
  p.product_category,
  SUM(oi.price) AS total_sales,
  COUNT(oi.order_id) AS sales_count,
  ROUND(SUM(oi.price) / COUNT(oi.order_id), 2) AS avg_price_per_sale, 
  RANK() OVER (ORDER BY SUM(oi.price) DESC) AS sales_priority -- ranking de prioridade em ordem
FROM 
  delta_scan('../delta_lake/silver/products_silver') p
  JOIN delta_scan('../delta_lake/silver/order_items_silver') oi ON p.product_id = oi.product_id
GROUP BY 
  p.product_id, p.product_category
ORDER BY 
  total_sales DESC;