SELECT
    leads_qualified.mql_id,
    CAST(leads_qualified.first_contact_date AS TIMESTAMP) AS first_contact_date,
    LOWER(TRIM(COALESCE(leads_qualified.origin, 'N/A'))) AS came_from,
    leads_closed.won_date,
    LOWER(TRIM(COALESCE(leads_closed.business_segment, 'N/A'))) AS business_segment,
    LOWER(TRIM(COALESCE(leads_closed.lead_type, 'N/A'))) AS lead_type,
    -- calculo de tempo para conversão dos leads (em dias)
    DATEDIFF(
        'day', 
        leads_qualified.first_contact_date::TIMESTAMP, 
        leads_closed.won_date::TIMESTAMP
    ) AS days_to_convert
FROM delta_scan('../delta_lake/bronze/leads_qualified_bronze') AS leads_qualified
LEFT JOIN delta_scan('../delta_lake/bronze/leads_closed_bronze') AS leads_closed
    ON leads_qualified.mql_id = leads_closed.mql_id;