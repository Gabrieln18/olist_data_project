SELECT
    seller_id,
    seller_zip_code_prefix AS seller_cep,
    LOWER(TRIM(seller_city)) AS seller_city,
    UPPER(TRIM(seller_state)) AS seller_state
FROM delta_scan('../delta_lake/bronze/sellers_bronze')