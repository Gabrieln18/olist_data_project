SELECT 
    mql_id,
    seller_id,
    sdr_id,
    sr_id,
    CAST(won_date AS TIMESTAMP) AS won_date,
    LOWER(TRIM(business_segment)) AS business_segment,
    LOWER(TRIM(lead_type)) AS lead_type,
    LOWER(TRIM(lead_behaviour_profile)) AS lead_behaviour_profile,
    COALESCE(has_company, 0) AS has_company,
    COALESCE(has_gtin, 0) AS has_gtin,
    COALESCE(NULLIF(average_stock, ''), 'N/A') AS average_stock,
    LOWER(TRIM(business_type)) AS business_type,
    COALESCE(declared_product_catalog_size, 0.0) AS declared_product_catalog_size,
    COALESCE(declared_monthly_revenue, 0.0) AS declared_monthly_revenue
FROM delta_scan('../delta_lake/bronze/leads_closed_bronze')