import duckdb
from deltalake import write_deltalake
import logging
import os
from pathlib import Path

# config logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def extract_func_pipeline(source_path_csv: str, bronze_delta_path: str, name_table: str, mode: str = "overwrite"):
    conn = duckdb.connect()
    
    try:
        logger.info(f"Iniciando extração do arquivo {source_path_csv}")
        dataframe = conn.sql(f"SELECT * FROM read_csv_auto('{source_path_csv}')")
        write_deltalake(bronze_delta_path, dataframe, mode=mode, name=name_table)
        logger.info(f"\033[32m[OK]\033[0m Processo de extração do {source_path_csv} foi concluído.")

    except Exception as e:
        logger.error(f"Erro ao processar {source_path_csv}: {str(e)}")

    finally:
        conn.close()  # isso evita o problema de memory leaks ao duckdb

if __name__ == "__main__":
    path_dir = Path("../data")
    files_list_path = [str(file) for file in path_dir.glob("*.csv")]

    # função para normalizar os nomes dos arquivos
    def extract_name(path: str) -> str:
        name = os.path.basename(path)  # pega só o nome do arquivo
        name = name.replace("olist_", "").replace("_dataset", "").replace(".csv", "")  # removendo os prefixos e sufixos
        return name

    # cria uma dict {nome_limpo: caminho_original}
    files_dict = {extract_name(path): path for path in files_list_path}

    bronze_delta_path = "../delta_lake/bronze/"
    os.makedirs(bronze_delta_path, exist_ok=True)  # Cria se não existir

    # itera sobre os arquivos
    for dataset, file_path in files_dict.items():
        bronze_delta_path = f"../delta_lake/bronze/{dataset}"
        extract_func_pipeline(file_path, bronze_delta_path, name_table=dataset)