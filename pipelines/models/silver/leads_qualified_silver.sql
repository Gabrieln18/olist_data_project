SELECT
    mql_id,
    landing_page_id,
    CAST(first_contact_date AS TIMESTAMP) AS first_contact_date,
    LOWER(TRIM(COALESCE(origin, 'N/A'))) AS came_from
FROM delta_scan('../delta_lake/bronze/leads_qualified_bronze');