SELECT 
  op.payment_type,
  ROUND(AVG(op.payment_value), 2) AS avg_payment_value,
  COUNT(DISTINCT o.order_id) AS total_orders,
  SUM(CASE WHEN o.order_status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_orders,
  SUM(CASE WHEN o.order_status = 'cancelled' THEN 1 ELSE 0 END) * 1.0 / COUNT(DISTINCT o.order_id) AS cancellation_rate,
  ROUND(AVG(oi.price + oi.freight_value), 2) AS avg_order_value
FROM 
  delta_scan('../delta_lake/silver/payments_silver') op
  JOIN delta_scan('../delta_lake/silver/orders_full_data_silver') o ON op.order_id = o.order_id
  JOIN delta_scan('../delta_lake/silver/order_items_silver') oi ON o.order_id = oi.order_id
GROUP BY 
  op.payment_type
ORDER BY 
  avg_order_value DESC, cancellation_rate ASC;