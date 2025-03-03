import duckdb as dd
from deltalake import write_deltalake
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IngestionPipeline:

    """
    Classe responsável pela ingestão de dados de arquivos CSV para tabelas Delta por meio do DuckDB.
    
    Attributes:
        name_pipeline (str): Nome do pipeline
        path_delta (str): Caminho onde a tabela Delta será armazenada
        path_csv (str): Caminho do arquivo CSV de origem
        mode (str): Modo de escrita para a tabela Delta ('overwrite', 'append', etc.)
    """

    def __init__(self, name_pipeline: str, name_table: str, path_delta: str, path_csv: str, mode: str = "overwrite"):
        self.path_delta = path_delta
        self.path_csv = path_csv
        self.mode = mode
        self.name_pipeline = name_pipeline
        self.name_table = name_table
        self.conn = dd.connect()

    def ingestion_to_bronze_delta(self):
        """Inicia o processo de ingestão dos dados de origem e gera um arquivo RAW em formato Delta Table"""
        logger.info(f"Iniciando ingestão do arquivo {self.path_csv}")
        dataframe = self.conn.sql(f"SELECT * FROM read_csv_auto('{self.path_csv}')")
        write_deltalake(self.path_delta, dataframe, mode= self.mode, name= self.name_table)
        print(f"\033[32m[INGESTION]\033[0m Data pipeline \033[0;33m'{self.name_pipeline}'\033[0m is runinng...")
        print(f"\033[32m[OK]\033[0m Delta table \033[0;33m{self.name_table}\033[0m was created successfully.")

    def read_delta_table(self, limit: int = 10):
        """Ler tabela do tipo Delta Table e realiza consulta com DuckDB"""
        logger.info(f"Lendo tabela Delta em {self.path_delta}")
        result = self.conn.sql(f"SELECT * FROM delta_scan('{self.path_delta}') LIMIT {limit}").fetchdf
        print(result)

    def describe_csv(self):
        """Realiza o DESCRIBE do DuckDB ao arquivo CSV"""
        logger.info(f"Analisando estrutura do arquivo {self.path_csv}")
        query = f"DESCRIBE SELECT * FROM read_csv_auto('{self.path_csv}')"
        result = self.conn.sql(query).fetchdf
        print(result)

    def read_csv(self, limit: int = 10):
        """Realiza consulta simple ao arquivo CSV de origem"""
        logger.info(f"Lendo tabela CSV em {self.path_csv}")
        query = f"SELECT * FROM read_csv_auto('{self.path_csv}') LIMIT {limit}"
        result = self.conn.sql(query).fetchdf
        print(result)


