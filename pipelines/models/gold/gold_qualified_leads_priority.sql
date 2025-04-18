WITH leads_origin AS (
  SELECT 
    came_from,
    COUNT(mql_id) AS total_leads
  FROM 
    delta_scan('../delta_lake/silver/leads_qualified_silver')
  GROUP BY 
    came_from
),
ranked_leads AS (
  SELECT 
    mql_id,
    came_from,
    ROW_NUMBER() OVER (ORDER BY came_from) AS row_num
  FROM 
    delta_scan('../delta_lake/silver/leads_qualified_silver')
)
SELECT 
  l.mql_id,
  l.first_contact_date,
  l.landing_page_id,
  l.came_from,
  lo.total_leads,
  -- calcular prioridade baseada na frequência
  CASE 
    WHEN lo.total_leads > (SELECT AVG(total_leads) FROM leads_origin) THEN 'high'
    WHEN lo.total_leads = (SELECT AVG(total_leads) FROM leads_origin) THEN 'median'
    ELSE 'low'
  END AS priority
FROM 
  delta_scan('../delta_lake/silver/leads_qualified_silver') l
JOIN 
  leads_origin lo ON l.came_from = lo.came_from
ORDER BY 
  lo.total_leads DESC;