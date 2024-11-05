import pandas as pd
from sqlalchemy import text
from dimensiones.db_connection import get_database_connections

def extract(source_db):
  try:
    query = """
        SELECT 
            s.sede_id as id_sede,
            s.nombre,
            s.direccion,
            s.telefono,
            s.nombre_contacto,
            c.nombre as ciudad,
            cl.cliente_id as id_cliente,
            cl.nombre as nombre_cliente
        FROM sede s
        LEFT JOIN cliente cl ON cl.cliente_id = s.cliente_id
        LEFT JOIN ciudad c ON c.ciudad_id = s.ciudad_id
    """
    
    data = pd.read_sql(query, source_db)
    print(f"Data extracted successfully from 'sede'. Records found: {len(data)}")
    #print(data)
    return data
  
  except Exception as e:
    print(f"Error extracting data: {str(e)}")

def transform(data):
  try:
    # handling null values
    data['nombre'] = data['nombre'].fillna('NO ESPECIFICADO')
    data['direccion'] = data['direccion'].fillna('NO ESPECIFICADO')
    data['telefono'] = data['telefono'].fillna('NO ESPECIFICADO')
    data['ciudad'] = data['ciudad'].fillna('NO ESPECIFICADO')
    data['id_cliente'] = data['id_cliente'].fillna('NO ESPECIFICADO')
    data['nombre_cliente'] = data['nombre_cliente'].fillna('NO ESPECIFICADO')
    
    data['nombre'] = data['nombre'].str.strip().str.upper()
    data['direccion'] = data['direccion'].str.strip().str.upper()
    data['nombre_contacto'] = data['nombre_contacto'].str.strip().str.upper()
    data['ciudad'] = data['ciudad'].str.strip().str.upper()
    data['nombre_cliente'] = data['nombre_cliente'].str.strip().str.upper()

    print(f"Data transformed successfully from 'sede'. Records found: {len(data)}")
    #print(data)
    return data
  
  except Exception as e:
    print(f"Error transforming data: {str(e)}")


def load(warehouse_db, transformed):
  try:
    table_query = text("""
      CREATE TABLE IF NOT EXISTS dim_sede (
        id_sede INTEGER PRIMARY KEY,
        nombre VARCHAR(100) NOT NULL,
        direccion VARCHAR(100) NOT NULL,
        telefono VARCHAR(100) NOT NULL,
        nombre_contacto VARCHAR(100) NOT NULL,
        ciudad VARCHAR(100) NOT NULL,
        id_cliente INTEGER NOT NULL,
        nombre_cliente VARCHAR(100) NOT NULL
      );
    """)
          
    with warehouse_db.connect() as conn:
        conn.execute(table_query)
    
    transformed.to_sql(
        'dim_sede',
        warehouse_db,
        if_exists='replace',
        index=False,
        method='multi'
    )
  except Exception as e:
    print(f"Error loading data: {str(e)}")

def run_etl_dim_sede():
  #get databases connections
  source_db, warehouse_db = get_database_connections()
  
  # extract the data
  data = extract(source_db)

  # transform data
  transformed = transform(data)

  # load to the warehouse
  load(warehouse_db, transformed)

if __name__ == "__main__":
    run_etl_dim_sede()