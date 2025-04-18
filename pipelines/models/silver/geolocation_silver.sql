SELECT
    geolocation_zip_code_prefix AS geolocation_cep,
    COALESCE(geolocation_lat, NULL) AS geolocation_lat,  -- Mantém NULL para evitar coordenadas erradas
    COALESCE(geolocation_lng, NULL) AS geolocation_lng,
    LOWER(TRIM(COALESCE(geolocation_city, 'N/A'))) AS geolocation_city,
    UPPER(TRIM(COALESCE(geolocation_state, 'N/A'))) AS geolocation_state
FROM delta_scan('../delta_lake/bronze/geolocation_bronze');