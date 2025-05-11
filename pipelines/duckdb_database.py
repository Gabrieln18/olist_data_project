import duckdb
import os

def create_database_persistant(duckdb_dir: str, file_path: str):
    """Cria um banco de dados persistente no diretório alvo
    
    Args:
        duckdb_dir (str): Caminho do Diretório onde o banco de dados será armazenado
        file_path (str): Diretório de arquivos Delta que serão armazenados no banco de dados
    
    Returns:
        DuckDBConnection: Conexão com o banco de dados persistente
    """
    import duckdb
    
    # conectar ao banco de dados persistente
    conn = duckdb.connect(duckdb_dir)
    conn.execute("INSTALL delta")
    conn.execute("LOAD delta")
    
    # Cria ou substitui tabelas para cada tabela Delta no diretório
    # Lista todas as pastas no diretório Delta (cada pasta é uma tabela)
    if os.path.isdir(file_path):
        delta_tables = [d for d in os.listdir(file_path) if os.path.isdir(os.path.join(file_path, d))]
        
        for table in delta_tables:
            table_path = os.path.join(file_path, table)
            conn.execute(f"""
                CREATE OR REPLACE TABLE {table} AS 
                SELECT * FROM delta_scan(?)
            """, [table_path])
        
        print(f"Carregadas {len(delta_tables)} tabelas Delta: {', '.join(delta_tables)}")
    else:
        table_name = os.path.basename(file_path)
        conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS 
            SELECT * FROM delta_scan(?)
        """, [file_path])
    
    return conn

def query_sql(query: str, database: str = ":memory:"):
    """Realiza consulta sql com duckdb"""
    conn = duckdb.connect(database)
    result = conn.sql(query).fetch_arrow_reader()
    print(result)
    conn.close()    

create_database_persistant(duckdb_dir="/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/data/gold_database.db", file_path="/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold")