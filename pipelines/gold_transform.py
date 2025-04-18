import os
import logging
import duckdb
from deltalake import write_deltalake

# Configuração do logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def gold_pipeline_sql(query: str, gold_table_name: str, mode: str = "overwrite", gold_path_out: str = "../delta_lake/gold/"):
    """Executa uma query SQL sobre um Delta Table Silver e salva o resultado na camada Gold."""
    
    gold_path = gold_path_out
    gold_path_delta = f"{gold_path}{gold_table_name}"
    os.makedirs(gold_path, exist_ok=True) # cria o diretório se não existir

    try:
        with duckdb.connect() as conn:
            logger.info(f"Executando transformação na tabela '{gold_table_name}' para a camada GOLD")
            df_transformed_gold = conn.sql(query).arrow()
            write_deltalake(gold_path_delta, df_transformed_gold, mode=mode)
            logger.info(f"\033[32m[OK]\033[0m Tabela '{gold_table_name}' processada com sucesso.")

    except Exception as e:
        logger.error(f"\033[31m[ERROR]\033[0m Erro inesperado ao processar '{gold_table_name}': {str(e)}")

    finally:
        if conn:
            conn.close()