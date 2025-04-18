SELECT
    CAST(product_id AS VARCHAR) AS product_id,
    COALESCE(LOWER(TRIM(product_category_name)), 'unknown') AS product_category,
    COALESCE(CAST(product_name_lenght AS INT), 0) AS product_name_length,
    COALESCE(CAST(product_description_lenght AS INT), 0) AS product_description_length,
    COALESCE(CAST(product_photos_qty AS INT), 0) AS product_photos_qty,
    COALESCE(CAST(product_weight_g AS INT), 0) AS product_weight_g,
    COALESCE(CAST(product_length_cm AS INT), 0) AS product_length_cm,
    COALESCE(CAST(product_height_cm AS INT), 0) AS product_height_cm,
    COALESCE(CAST(product_width_cm AS INT), 0) AS product_width_cm
FROM delta_scan('../delta_lake/bronze/products_bronze');