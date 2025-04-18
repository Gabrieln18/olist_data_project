SELECT 
    order_id,
    payment_sequential,
    LOWER(TRIM(COALESCE(payment_type, 'N/A'))) AS payment_type, 
    COALESCE(payment_installments, 0) AS payment_installments,
    COALESCE(payment_value, 0) AS payment_value
FROM delta_scan('../delta_lake/bronze/payments_bronze');