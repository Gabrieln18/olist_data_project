import duckdb


def read_delta_table(path_delta: str, limit: int = 10):
    """Ler tabela do tipo Delta Table e realiza consulta com DuckDB"""
    conn = duckdb.connect()
    result = conn.sql(f"SELECT * FROM delta_scan('{path_delta}') LIMIT {limit}").fetchdf
    print(result)
    conn.close()

def describe_csv(path_csv: str):
    """Realiza o DESCRIBE do DuckDB ao arquivo CSV"""
    conn = duckdb.connect()
    query = f"DESCRIBE SELECT * FROM read_csv_auto('{path_csv}')"
    result = conn.sql(query).fetchdf
    print(result)
    conn.close()

def read_csv(path_csv: str, limit: int = 10):
    """Realiza consulta simple ao arquivo CSV de origem"""
    conn = duckdb.connect()
    query = f"SELECT * FROM read_csv_auto('{path_csv}') LIMIT {limit}"
    result = conn.sql(query).fetchdf
    print(result)
    conn.close()

def query_sql(query: str):
    """Realiza consulta sql com duckdb"""
    conn = duckdb.connect()
    result = conn.sql(query).fetchdf
    print(result)
    conn.close()

def get_describe_delta(path_delta: str):
    conn = duckdb.connect()
    query = f"DESCRIBE SELECT * FROM delta_scan('{path_delta}')"
    result = conn.sql(query).fetchdf
    print(result)
    conn.close()