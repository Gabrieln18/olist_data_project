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
