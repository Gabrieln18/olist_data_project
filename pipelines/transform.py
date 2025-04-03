import os
import logging
import duckdb
from deltalake import write_deltalake

# Configuração do logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def transform_pipeline_sql(query: str, table_name: str, mode: str = "overwrite"):
    """Executa uma query SQL sobre um Delta Table e salva o resultado na camada Silver."""
    
    silver_path = "../delta_lake/silver/"
    silver_path_delta = f"{silver_path}{table_name}"
    os.makedirs(silver_path, exist_ok=True) # cria o diretório se não existir

    try:
        with duckdb.connect() as conn:
            logger.info(f"Executando transformação na tabela '{table_name}'")
            df_transformed = conn.sql(query).arrow()
            write_deltalake(silver_path_delta, df_transformed, mode=mode) # gravando o resultado em uma delta table
            logger.info(f"\033[32m[OK]\033[0m Tabela '{table_name}' processada com sucesso.")

    except Exception as e:
        logger.error(f"\033[31m[ERROR]\033[0m Erro inesperado ao processar '{table_name}': {str(e)}")

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":

    # customers table
    transform_pipeline_sql(
        query=f"""
        SELECT
            customer_id,
            customer_unique_id,
            LOWER(TRIM(customer_zip_code_prefix)) AS customer_cep,
            LOWER(TRIM(customer_city)) AS customer_city,
            UPPER(TRIM(customer_state)) AS customer_state,
        FROM delta_scan('../delta_lake/bronze/customers_bronze')
        """,
        table_name="customers_silver"
    )

    # geolocation table
    transform_pipeline_sql(
        query=f"""
        SELECT
            geolocation_zip_code_prefix AS geolocation_cep,
            COALESCE(geolocation_lat, NULL) AS geolocation_lat,  -- Mantém NULL para evitar coordenadas erradas
            COALESCE(geolocation_lng, NULL) AS geolocation_lng,
            LOWER(TRIM(COALESCE(geolocation_city, 'N/A'))) AS geolocation_city,
            UPPER(TRIM(COALESCE(geolocation_state, 'N/A'))) AS geolocation_state
        FROM delta_scan('../delta_lake/bronze/geolocation_bronze');
        """, 
        table_name="geolocation_silver"
    )
    
    # order_items table
    transform_pipeline_sql(
        query=f"""
        SELECT 
            order_id,
            order_item_id,
            product_id,
            seller_id,
            CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_date,
            CAST(price AS DOUBLE) AS price,
            CAST(freight_value AS DOUBLE) AS freight_value
        FROM delta_scan('../delta_lake/bronze/order_items_bronze')
        """,
        table_name="order_items_silver"
    )


    # payments table
    transform_pipeline_sql(query="""
        SELECT 
            order_id,
            payment_sequential,
            LOWER(TRIM(COALESCE(payment_type, 'N/A'))) AS payment_type, 
            COALESCE(payment_installments, 0) AS payment_installments,
            COALESCE(payment_value, 0) AS payment_value
        FROM delta_scan('../delta_lake/bronze/payments_bronze');
    """, table_name="payments_silver")

    # orders reviews table
    transform_pipeline_sql(query="""
    SELECT 
        review_id,
        order_id,
        CAST(review_score AS INT) AS review_score,
        COALESCE(review_comment_title, 'no_title') AS review_comment_title,
        COALESCE(review_comment_message, 'no_message') AS review_comment_message,
        CAST(review_creation_date AS TIMESTAMP) AS review_creation_date,
        CAST(review_answer_timestamp AS TIMESTAMP) AS review_answer_timestamp
    FROM delta_scan('../delta_lake/bronze/reviews_bronze');
    """, table_name="reviews_silver")

    # orders table - transformar e retornar delta table com APENAS PEDIDOS ENTREGUES
    transform_pipeline_sql(
        query=f"""
        SELECT
            CAST(order_id AS VARCHAR) AS order_id,
            CAST(customer_id AS VARCHAR) AS customer_id,
            CAST(order_status AS VARCHAR) AS order_status,
            CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp,
            CAST(order_approved_at AS TIMESTAMP) AS order_approved_at,
            CAST(order_delivered_carrier_date AS TIMESTAMP) AS order_delivered_carrier_date,
            CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_date,
            CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_date
        FROM delta_scan('../delta_lake/bronze/orders_bronze')
        WHERE order_status = 'delivered'
        """,
        table_name="orders_only_delivered_silver"
    )

    # orders table - transformar e retornar delta table com TODOS OS DADOS
    transform_pipeline_sql(
        query=f"""
        SELECT
            CAST(order_id AS VARCHAR) AS order_id,
            CAST(customer_id AS VARCHAR) AS customer_id,
            CAST(order_status AS VARCHAR) AS order_status,
            CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp,
            CAST(order_approved_at AS TIMESTAMP) AS order_approved_at,
            CAST(order_delivered_carrier_date AS TIMESTAMP) AS order_delivered_carrier_date,
            CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_date,
            CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_date
        FROM delta_scan('../delta_lake/bronze/orders_bronze')
        """,
        table_name="orders_full_data_silver"
    )

    # products table
    transform_pipeline_sql(
        query=f"""
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
        """,
        table_name="products_silver"
    )

    # order_reviews table
    transform_pipeline_sql(query=f"""
            SELECT 
                CAST(order_id AS VARCHAR) AS order_id,
                CAST(review_id AS VARCHAR) AS review_id,
                COALESCE(CAST(review_score AS INT), 0) AS review_score,  -- Substitui NULL por 0
                COALESCE(LOWER(TRIM(review_comment_title)), 'sem título') AS review_comment_title, -- Substitui NULL por 'sem título'
                COALESCE(LOWER(TRIM(review_comment_message)), 'sem comentário') AS review_comment_message, -- Substitui NULL por 'sem comentário'
                COALESCE(CAST(review_creation_date AS TIMESTAMP), CURRENT_TIMESTAMP) AS review_creation_date, -- Se NULL, usa data atual
                COALESCE(CAST(review_answer_timestamp AS TIMESTAMP), CURRENT_TIMESTAMP) AS review_answer_timestamp -- Se NULL, usa data atual
            FROM delta_scan('../delta_lake/bronze/order_reviews')
    """, table_name="order_reviews")

    # orders table - transformar e retornar delta table com apenas os PEDIDOS ENTREGUES
    transform_pipeline_sql(
        query=f"""
        SELECT
            CAST(order_id AS VARCHAR) AS order_id,
            CAST(customer_id AS VARCHAR) AS customer_id,
            CAST(order_status AS VARCHAR) AS order_status,
            CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp,
            CAST(order_approved_at AS TIMESTAMP) AS order_approved_at,
            CAST(order_delivered_carrier_date AS TIMESTAMP) AS order_delivered_carrier_date,
            CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_date,
            CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_date
        FROM delta_scan('../delta_lake/bronze/orders_bronze')
        WHERE order_status = 'delivered'
        """,
        table_name="orders_only_delivered_silver"
    )
    
    # orders table - transformar e retornar delta table com TODOS OS DADOS
    transform_pipeline_sql(
        query=f"""
        SELECT
            CAST(order_id AS VARCHAR) AS order_id,
            CAST(customer_id AS VARCHAR) AS customer_id,
            CAST(order_status AS VARCHAR) AS order_status,
            CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp,
            CAST(order_approved_at AS TIMESTAMP) AS order_approved_at,
            CAST(order_delivered_carrier_date AS TIMESTAMP) AS order_delivered_carrier_date,
            CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_date,
            CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_date
        FROM delta_scan('../delta_lake/bronze/orders_bronze')
        """,
        table_name="orders_full_data_silver"
    )

    # products table
    transform_pipeline_sql(query=f"""
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
    """, table_name="products")

    # sellers table
    transform_pipeline_sql(query=f"""
        SELECT
            seller_id,
            seller_zip_code_prefix AS seller_cep,
            LOWER(TRIM(seller_city)) AS seller_city,
            UPPER(TRIM(seller_state)) AS seller_state
        FROM delta_scan('../delta_lake/bronze/sellers_bronze')
    """, table_name="sellers")

    # leads qualified table
    transform_pipeline_sql(
        query="""
        SELECT
            mql_id,
            landing_page_id,
            CAST(first_contact_date AS TIMESTAMP) AS first_contact_date,
            LOWER(TRIM(COALESCE(origin, 'N/A'))) AS came_from
        FROM delta_scan('../delta_lake/bronze/leads_qualified_bronze');
    """, table_name="leads_qualified_silver"
    )

    # leads_closed table
    transform_pipeline_sql(
        query="""
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
        """,
        table_name="leads_closed_silver"
    )
