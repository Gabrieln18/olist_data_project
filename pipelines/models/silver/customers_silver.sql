SELECT
    customer_id,
    customer_unique_id,
    LOWER(TRIM(customer_zip_code_prefix)) AS customer_cep,
    LOWER(TRIM(customer_city)) AS customer_city,
    UPPER(TRIM(customer_state)) AS customer_state,
FROM delta_scan('../delta_lake/bronze/customers_bronze')