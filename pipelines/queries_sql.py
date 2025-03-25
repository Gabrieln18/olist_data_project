from utils import query_sql

# consultar total de nulls em orders_reviews
query_sql("SELECT COUNT(*) AS nulls_total FROM delta_scan('../delta_lake/bronze/order_reviews') WHERE review_creation_date IS NULL")
query_sql("SELECT COUNT(*) AS nulls_total FROM delta_scan('../delta_lake/bronze/order_reviews') WHERE review_answer_timestamp IS NULL")

# verificar as linhas nulls em orders com o status em invoiced
query_sql("SELECT * FROM delta_scan('../delta_lake/bronze/orders') WHERE order_status = 'invoiced' LIMIT 10")

# consulta sobre a contagem de nulls no dataset products.
query_sql("""SELECT 
    COUNT(*) FILTER (WHERE product_category_name IS NULL) AS product_category_nulls,
    COUNT(*) FILTER (WHERE product_name_lenght IS NULL) AS product_name_length_nulls,
    COUNT(*) FILTER (WHERE product_description_lenght IS NULL) AS product_description_length_nulls,
    COUNT(*) FILTER (WHERE product_photos_qty IS NULL) AS product_photos_qty_nulls,
    COUNT(*) FILTER (WHERE product_weight_g IS NULL) AS product_weight_g_nulls,
    COUNT(*) FILTER (WHERE product_length_cm IS NULL) AS product_length_cm_nulls,
    COUNT(*) FILTER (WHERE product_height_cm IS NULL) AS product_height_cm_nulls,
    COUNT(*) FILTER (WHERE product_width_cm IS NULL) AS product_width_cm_nulls
FROM delta_scan('../delta_lake/bronze/products');
""")