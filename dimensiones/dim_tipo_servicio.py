import pandas as pd
from sqlalchemy import text
from dimensiones.db_connection import get_database_connections

def extract(source_db):
  try:
    query = """
        SELECT 
            id as id_tipo_servicio,
            nombre,
            descripcion
        FROM mensajeria_tiposervicio
    """
    
    data = pd.read_sql(query, source_db)
    print(f"Data extracted successfully from 'mensajeria_tiposervicio'. Records found: {len(data)}")
    return data
  
  except Exception as e:
    print(f"Error extracting data: {str(e)}")

def transform(data):
  try:
    # handling null values
    if data['nombre'].isnull().any():
      raise ValueError("Null values ​​were found in critical fields")
    data['nombre'] = data['nombre'].fillna('NO ESPECIFICADO')
    data['descripcion'] = data['descripcion'].fillna('NO ESPECIFICADO')
    
    data['nombre'] = data['nombre'].str.strip().str.upper()
    data['descripcion'] = data['descripcion'].str.strip().str.upper()

    print(f"Data transformed successfully from 'mensajeria_tiposervicio'. Records found: {len(data)}")
    return data
  
  except Exception as e:
    print(f"Error transforming data: {str(e)}")


def load(warehouse_db, transformed):
  try:
    table_query = text("""
      CREATE TABLE IF NOT EXISTS dim_tipo_servicio (
        id_tipo_servicio INTEGER PRIMARY KEY,
        nombre VARCHAR(100) NOT NULL,
        descripcion VARCHAR(100) NOT NULL
      );
    """)
          
    with warehouse_db.connect() as conn:
        conn.execute(table_query)
    
    transformed.to_sql(
        'dim_tipo_servicio',
        warehouse_db,
        if_exists='replace',
        index=False,
        method='multi'
    )
  except Exception as e:
    print(f"Error loading data: {str(e)}")

def run_etl_dim_tipo_servicio():
  #get databases connections
  source_db, warehouse_db = get_database_connections()
  
  # extract the data
  data = extract(source_db)

  # transform data
  transformed = transform(data)

  # load to the warehouse
  load(warehouse_db, transformed)

if __name__ == "__main__":
    run_etl_dim_tipo_servicio()