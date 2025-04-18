SELECT
    c.customer_id,
    c.customer_unique_id,
    c.customer_cep,
    c.customer_city,
    c.customer_state,
    AVG(g.geolocation_lat) AS avg_latitude,
    AVG(g.geolocation_lng) AS avg_longitude
FROM delta_scan('../delta_lake/silver/customers_silver') AS c
LEFT JOIN delta_scan('../delta_lake/silver/geolocation_silver') AS g
ON c.customer_cep = g.geolocation_cep
GROUP BY
    c.customer_id,
    c.customer_unique_id,
    c.customer_cep,
    c.customer_city,
    c.customer_state;