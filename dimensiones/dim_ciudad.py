import pandas as pd
from sqlalchemy import text
from dimensiones.db_connection import get_database_connections

def extract(source_db):
  try:
    query = """
        SELECT 
            c.ciudad_id as id_ciudad,
            c.nombre as ciudad,
            d.nombre as departamento
        FROM ciudad c
        LEFT JOIN departamento d ON c.departamento_id = d.departamento_id
    """
    
    data = pd.read_sql(query, source_db)
    print(f"Data extracted successfully from 'ciudad'. Records found: {len(data)}")
    print(data)
    return data
  
  except Exception as e:
    print(f"Error extracting data: {str(e)}")

def transform(data):
  try:
    # handling null values
    if data['ciudad'].isnull().any():
      raise ValueError("Null values ​​were found in critical fields")
    data['departamento'] = data['departamento'].fillna('NO ESPECIFICADO')
    
    data['ciudad'] = data['ciudad'].str.strip().str.upper()
    data['departamento'] = data['departamento'].str.strip().str.upper()

    print(f"Data transformed successfully from 'ciudad'. Records found: {len(data)}")
    print(data)
    return data
  
  except Exception as e:
    print(f"Error transforming data: {str(e)}")


def load(warehouse_db, transformed):
  try:
    table_query = text("""
      CREATE TABLE IF NOT EXISTS dim_ciudad (
        id_ciudad INTEGER PRIMARY KEY,
        ciudad VARCHAR(100) NOT NULL,
        departamento VARCHAR(100) NOT NULL
      );
    """)
          
    with warehouse_db.connect() as conn:
        conn.execute(table_query)
    
    transformed.to_sql(
        'dim_ciudad',
        warehouse_db,
        if_exists='replace',
        index=False,
        method='multi'
    )
  except Exception as e:
    print(f"Error loading data: {str(e)}")

def run_etl_dim_ciudad():
  #get databases connections
  source_db, warehouse_db = get_database_connections()
  
  # extract the data
  data = extract(source_db)

  # transform data
  transformed = transform(data)

  # load to the warehouse
  load(warehouse_db, transformed)

if __name__ == "__main__":
    run_etl_dim_ciudad()