import pandas as pd
from dimensiones.db_connection import get_database_connections
from sqlalchemy import create_engine, text
from datetime import datetime
    
def extract(source_db, warehouse_db):
  try:
    query = """
      select m.fecha_solicitud, m.cliente_id, m.mensajero_id, m.tipo_servicio_id, m.ciudad_origen_id, m.ciudad_destino_id, s.sede_id , count(usuario_id) as total_finalizado_mensajero
      from mensajeria_servicio m
      inner join clientes_usuarioaquitoy cu on cu.user_id = usuario_id
      inner join sede s on s.sede_id = cu.sede_id 
      where asignar_mensajero = false
      group by m.fecha_solicitud, m.cliente_id, m.mensajero_id, m.tipo_servicio_id, m.ciudad_origen_id, m.ciudad_destino_id, m.usuario_id, s.sede_id  ;
    """
    
    data = pd.read_sql(query, source_db)

    print(f"Data extracted successfully from 'mensajeria_servicio'. Records found: {len(data)}")
    return data
  
  except Exception as e:
    print(f"Error extracting data: {str(e)}")
    raise
        
def transform(data, warehouse_db):
  try:    
    dim_fecha = pd.read_sql_table('dim_fecha', warehouse_db)

    data['fecha_solicitud'] = pd.to_datetime(data['fecha_solicitud']).dt.date
    dim_fecha['date'] = pd.to_datetime(dim_fecha['date']).dt.date

    data = pd.merge(data, dim_fecha[['date', 'key_dim_fecha']], left_on='fecha_solicitud', right_on='date', how='left')
    data.drop(columns=['date'], inplace=True)
    data.drop(columns=['fecha_solicitud'], inplace=True)
    data.rename(columns={'key_dim_fecha': 'id_fecha'}, inplace=True)

    print(f"Data transformed successfully. Records found: {len(data)}")
    return data
  
  except Exception as e:
    print(f"Error transforming data: {str(e)}")
        
def load(transformed, warehouse_db):
  try:
    table_query = text("""
      CREATE TABLE IF NOT EXISTS servicio_mensajeria_diario (
        id_cliente INTEGER,
        id_fecha INTEGER NOT NULL,
        id_tipo_servicio INTEGER NOT NULL,
        id_ciudad_destino INTEGER NOT NULL,
        id_ciudad_origen INTEGER NOT NULL,
        id_sede INTEGER NOT NULL,
        id_mensajero INTEGER NOT NULL,
        total_finalizado_mensajero INTEGER NOT NULL
      );
    """)
          
    with warehouse_db.connect() as conn:
        conn.execute(table_query)
    
    transformed.to_sql(
        'servicio_mensajeria_diario',
        warehouse_db,
        if_exists='replace',
        index_label='id',
        method='multi'
    )
  except Exception as e:
    print(f"Error loading data in servicio_mensa: {str(e)}")
    raise
        
def run_etl_servicio_mensajeria_diario():
  #get databases connections
  source_db, warehouse_db = get_database_connections()
  
  # extract the data
  data = extract(source_db, warehouse_db)

  # transform data
  transformed = transform(data, warehouse_db)

  # load to the warehouse
  load(transformed, warehouse_db)

if __name__ == "__main__":
    run_etl_servicio_mensajeria_diario()