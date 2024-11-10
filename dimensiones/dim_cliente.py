import pandas as pd
from sqlalchemy import text
from dimensiones.db_connection import get_database_connections

def extract(source_db):
  try:
    query = """
        SELECT 
            cl.cliente_id as id_cliente,
            cl.nit_cliente,
            cl.nombre,
            cl.email,
            cl.direccion,
            cl.telefono,
            cl.nombre_contacto,
            c.nombre as ciudad,
            t.nombre as tipo_cliente,
            cl.activo,
            cl.coordinador_id as id_coordinador,
            cl.sector
        FROM cliente cl
        LEFT JOIN tipo_cliente t ON t.tipo_cliente_id = cl.tipo_cliente_id
        LEFT JOIN ciudad c ON c.ciudad_id = cl.ciudad_id
    """
    
    data = pd.read_sql(query, source_db)
    print(f"Data extracted successfully from 'cliente'. Records found: {len(data)}")
    return data
  
  except Exception as e:
    print(f"Error extracting data: {str(e)}")

def transform(data):
  try:
    # handling null values
    if data['nit_cliente'].isnull().any() or data['nombre'].isnull().any() or data['direccion'].isnull().any() or data['telefono'].isnull().any() or data['ciudad'].isnull().any():
      raise ValueError("Null values ​​were found in critical fields")

    data['email'] = data['email'].fillna('NO ESPECIFICADO')
    data['nombre_contacto'] = data['nombre_contacto'].fillna('NO ESPECIFICADO')
    data['tipo_cliente'] = data['tipo_cliente'].fillna('NO ESPECIFICADO')
    data['id_coordinador'] = data['id_coordinador'].fillna(0)
    data['sector'] = data['sector'].fillna('NO ESPECIFICADO')
    
    data['nombre'] = data['nombre'].str.strip().str.upper()
    data['direccion'] = data['direccion'].str.strip().str.upper()
    data['nombre_contacto'] = data['nombre_contacto'].str.strip().str.upper()
    data['ciudad'] = data['ciudad'].str.strip().str.upper()
    data['tipo_cliente'] = data['tipo_cliente'].str.strip().str.upper()
    data['sector'] = data['sector'].str.strip().str.upper()

    print(f"Data transformed successfully from 'cliente'. Records found: {len(data)}")
    return data
  
  except Exception as e:
    print(f"Error transforming data: {str(e)}")


def load(warehouse_db, transformed):
  try:
    table_query = text("""
      CREATE TABLE IF NOT EXISTS dim_cliente (
        id_cliente INTEGER PRIMARY KEY,
        nit_cliente VARCHAR(100) NOT NULL,
        nombre VARCHAR(100) NOT NULL,
        email VARCHAR(100) NOT NULL,
        direccion VARCHAR(100) NOT NULL,
        telefono VARCHAR(100) NOT NULL,
        nombre_contacto VARCHAR(100) NOT NULL,
        ciudad VARCHAR(100) NOT NULL,
        tipo_cliente VARCHAR(100) NOT NULL,
        activo BOOL,
        id_coordinador INTEGER NOT NULL,
        sector VARCHAR(100) NOT NULL
      );
    """)
          
    with warehouse_db.connect() as conn:
        conn.execute(table_query)
    
    transformed.to_sql(
        'dim_cliente',
        warehouse_db,
        if_exists='replace',
        index=False,
        method='multi'
    )
  except Exception as e:
    print(f"Error loading data: {str(e)}")

def run_etl_dim_cliente():
  #get databases connections
  source_db, warehouse_db = get_database_connections()
  
  # extract the data
  data = extract(source_db)

  # transform data
  transformed = transform(data)

  # load to the warehouse
  load(warehouse_db, transformed)

if __name__ == "__main__":
    run_etl_dim_cliente()