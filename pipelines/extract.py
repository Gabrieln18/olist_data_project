import duckdb
from deltalake import write_deltalake
import logging
import os

# config logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def extract_func_csv(source_path_csv: str, bronze_delta_path: str, name_table: str, mode: str = "overwrite"):
    conn = duckdb.connect()
    
    try:
        logger.info(f"Iniciando extração do arquivo {source_path_csv}")        
        dataframe = conn.sql(f"SELECT * FROM read_csv_auto('{source_path_csv}')").arrow()
        table_path = os.path.join(bronze_delta_path, name_table)
        os.makedirs(table_path, exist_ok=True)
        write_deltalake(table_path, dataframe, mode=mode)
        logger.info(f"\033[32m[OK]\033[0m Processo de extração do .CSV {source_path_csv} foi concluído.")

    except Exception as e:
        logger.error(f"\033[031m[ERROR]\033[0m Erro ao processar {source_path_csv}: {str(e)}")

    finally:
        conn.close() # evita o problema de leaks memory ao duckdb

def extract_func_sqlite(db_path: str, sqlite_table: str, bronze_delta_path: str, name_table: str, mode: str = "overwrite"):
    conn = duckdb.connect()
    
    try:
        logger.info(f"Iniciando extração da tabela SQLite - {sqlite_table}")
        # conectar com sqlite usando duckdb
        conn.execute(f"ATTACH '{db_path}' AS sqlite_db;")
        dataframe = conn.sql(f"SELECT * FROM sqlite_db.{sqlite_table}").arrow()  # converte para PyArrow Table
        table_path = os.path.join(bronze_delta_path, name_table)
        os.makedirs(table_path, exist_ok=True)
        write_deltalake(table_path, dataframe, mode=mode)
        logger.info(f"\033[32m[OK]\033[0m Extração da tabela {sqlite_table} concluída e salva em {table_path}.")

    except Exception as e:
        logger.error(f"\033[031m[ERROR]\033[0m Erro ao processar {sqlite_table}: {str(e)}")

    finally:
        conn.close()

if __name__ == "__main__":

    # extração de dados .csv para camada bronze
    extract_func_csv(source_path_csv="../data/olist_customers_dataset.csv", name_table="customers_bronze", bronze_delta_path="../delta_lake/bronze")
    extract_func_csv(source_path_csv="../data/olist_geolocation_dataset.csv", name_table="geolocation_bronze", bronze_delta_path="../delta_lake/bronze")
    extract_func_csv(source_path_csv="../data/olist_order_items_dataset.csv", name_table="order_items_bronze", bronze_delta_path="../delta_lake/bronze")
    extract_func_csv(source_path_csv="../data/olist_order_payments_dataset.csv", name_table="payments_bronze", bronze_delta_path="../delta_lake/bronze")
    extract_func_csv(source_path_csv="../data/olist_order_reviews_dataset.csv", name_table="reviews_bronze", bronze_delta_path="../delta_lake/bronze")
    extract_func_csv(source_path_csv="../data/olist_orders_dataset.csv", name_table="orders_bronze", bronze_delta_path="../delta_lake/bronze")
    extract_func_csv(source_path_csv="../data/olist_products_dataset.csv", name_table="products_bronze", bronze_delta_path="../delta_lake/bronze")
    extract_func_csv(source_path_csv="../data/olist_sellers_dataset.csv", name_table="sellers_bronze", bronze_delta_path="../delta_lake/bronze")
    extract_func_csv(source_path_csv="../data/product_category_name_translation.csv", name_table="product_category_name_translation_bronze", bronze_delta_path="../delta_lake/bronze")

    # extração de dados sqlite, tabelas: leads_closed e leads_qualified
    extract_func_sqlite(db_path="../data/olist.sqlite", sqlite_table="leads_qualified", name_table="leads_qualified_bronze", bronze_delta_path="../delta_lake/bronze")
    extract_func_sqlite(db_path="../data/olist.sqlite", sqlite_table="leads_closed", name_table="leads_closed_bronze", bronze_delta_path="../delta_lake/bronze")