import duckdb
from deltalake import write_deltalake
# from utils import read_delta_table
# from utils import query_sql
# from utils import get_describe_delta
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def transform_table(query: str, table_name: str):
    """Transforma e salva delta tables para o silver"""
    conn = duckdb.connect()
    try:
        df_transformed = conn.sql(query).fetch_arrow_table()
        # criando o dir caso não exista
        silver_path = "../delta_lake/silver/"
        os.makedirs(silver_path, exist_ok=True)
        # salvando o delta table silver
        silver_path_delta = f"{silver_path}{table_name}"
        write_deltalake(silver_path_delta, df_transformed, mode="overwrite")
        logger.info(f"\033[32m[OK]\033[0m Tabela '{table_name}' processada com sucesso.")

    except Exception as e:
        logger.error(f"\033[31m[ERROR]\033[0m Erro ao processar '{table_name}': {str(e)}")

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":

    # customers table
    transform_table(query="""
        SELECT DISTINCT
            customer_id, 
            customer_unique_id,
            LOWER(TRIM(customer_city)) AS customer_city,
            UPPER(TRIM(customer_state)) AS customer_state
        FROM delta_scan('../delta_lake/bronze/customers')
    """, table_name="customers_silver")

    # geolocation table
    transform_table(query=f"""
            SELECT DISTINCT
                geolocation_zip_code_prefix,
                geolocation_lat,
                geolocation_lng,
                LOWER(TRIM(geolocation_city)) AS geolocation_city,
                UPPER(TRIM(geolocation_state)) AS geolocation_state
            FROM delta_scan('../delta_lake/bronze/geolocation')
            WHERE geolocation_lat IS NOT NULL 
            AND geolocation_lng IS NOT NULL
        """, table_name="geolocation")
    
    # order_items table
    transform_table(query=f"""
            SELECT 
                order_id,
                order_item_id,
                product_id,
                seller_id,
                CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_date,
                CAST(price AS DOUBLE) AS price,
                CAST(freight_value AS DOUBLE) AS freight_value
            FROM delta_scan('../delta_lake/bronze/order_items')
        """, table_name="order_items")

    # order_payments table
    transform_table(query=f"""
            SELECT 
                CAST(order_id AS VARCHAR) AS order_id, 
                CAST(payment_sequential AS INT) AS payment_sequential,
                LOWER(TRIM(payment_type)) AS payment_type, 
                CAST(payment_installments AS INT) AS payment_installments,
                CAST(payment_value AS DOUBLE) AS payment_value
            FROM delta_scan('../delta_lake/bronze/order_payments')
    """, table_name="order_payments")

    # order_reviews table
    transform_table(query=f"""
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
    transform_table(query=f"""
        SELECT
            CAST(order_id AS VARCHAR) AS order_id,
            CAST(customer_id AS VARCHAR) AS customer_id,
            CAST(order_status AS VARCHAR) AS order_status,
            CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp,
            CAST(order_approved_at AS TIMESTAMP) AS order_approved_at,
            CAST(order_delivered_carrier_date AS TIMESTAMP) AS order_delivered_carrier_date,
            CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_date,
            CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_date
        FROM delta_scan('../delta_lake/bronze/orders')
        WHERE order_status = 'delivered'
    """, table_name="orders_only_delivered")
    
    # orders table - transformar e retornar delta table com TODOS OS DADOS
    transform_table(query=f"""
        SELECT
            CAST(order_id AS VARCHAR) AS order_id,
            CAST(customer_id AS VARCHAR) AS customer_id,
            CAST(order_status AS VARCHAR) AS order_status,
            CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp,
            CAST(order_approved_at AS TIMESTAMP) AS order_approved_at,
            CAST(order_delivered_carrier_date AS TIMESTAMP) AS order_delivered_carrier_date,
            CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_date,
            CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_date
        FROM delta_scan('../delta_lake/bronze/orders')
    """, table_name="orders_full_data")

    # products table
    transform_table(query=f"""
        SELECT
            CAST(product_id AS VARCHAR) AS product_id,
            COALESCE(LOWER(TRIM(product_category_name)), 'unknown') AS product_category_name,
            COALESCE(CAST(product_name_lenght AS INT), 0) AS product_name_length,
            COALESCE(CAST(product_description_lenght AS INT), 0) AS product_description_length,
            COALESCE(CAST(product_photos_qty AS INT), 0) AS product_photos_qty,
            COALESCE(CAST(product_weight_g AS INT), 0) AS product_weight_g,
            COALESCE(CAST(product_length_cm AS INT), 0) AS product_length_cm,
            COALESCE(CAST(product_height_cm AS INT), 0) AS product_height_cm,
            COALESCE(CAST(product_width_cm AS INT), 0) AS product_width_cm
        FROM delta_scan('../delta_lake/bronze/products');
    """, table_name="products")

    # sellers table
    transform_table(query=f"""
        SELECT
            CAST(seller_id AS VARCHAR) AS seller_id,
            CAST(seller_zip_code_prefix AS VARCHAR) AS seller_cep_code,
            LOWER(TRIM(seller_city)) AS seller_city,
            UPPER(TRIM(seller_state)) AS seller_state
        FROM delta_scan('../delta_lake/bronze/sellers')
    """, table_name="sellers")
