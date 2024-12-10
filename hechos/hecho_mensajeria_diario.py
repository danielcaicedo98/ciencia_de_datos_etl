import pandas as pd
from dimensiones.db_connection import get_database_connections
from sqlalchemy import create_engine, text
from datetime import datetime
    
def extract(warehouse_db):
  try:
    dim_servicio = pd.read_sql_table('trans_servicio', warehouse_db)
    dim_cliente = pd.read_sql_table('dim_cliente', warehouse_db)
    dim_sede = pd.read_sql_table('dim_sede', warehouse_db)   
    
    
    columnas_sede = [
        "id_sede", "id_cliente",   
    ]    
    # Filtramos las columnas, asegurándonos de que existan
    dim_sede = dim_sede[columnas_sede].dropna(axis=1, how='all')
    
    columnas_cliente = [
        "id_cliente",
    ]    
    # Filtramos las columnas, asegurándonos de que existan
    dim_cliente = dim_cliente[columnas_cliente].dropna(axis=1, how='all')   
    
    columnas_servicio = [
        'servicio_id', 'cliente_id','fecha_iniciado','mensajero_id','sede_id'
    ]    
    dim_servicio = dim_servicio[columnas_servicio].dropna(axis=1, how='all')     
   
    df_servicio = pd.merge(dim_servicio, dim_cliente, left_on='cliente_id', right_on='id_cliente', how='left')
    df_servicio.drop(columns=['id_cliente'], inplace=True)    
    
    df_servicio.rename(columns={
      'fecha_iniciado': 'fecha_solicitud'
    }, inplace=True)
    
    columnas_a_conservar = [
        "cliente_id", "mensajero_id", "sede_id", "fecha_solicitud",'servicio_id'              
    ]    
    df_servicio = df_servicio[columnas_a_conservar].dropna(axis=1, how='all')    
       
    return df_servicio
  
  except Exception as e:
    print(f"Error extracting data: {str(e)}")
    raise
        
def transform(data, warehouse_db):
    try:
        dim_fecha = pd.read_sql_table('dim_fecha', warehouse_db)

        data['fecha_solicitud'] = pd.to_datetime(data['fecha_solicitud']).dt.date
        dim_fecha['date'] = pd.to_datetime(dim_fecha['date']).dt.date
        # Convertir fecha_solicitud a formato de fecha
        data['fecha_solicitud'] = pd.to_datetime(data['fecha_solicitud']).dt.date
        
        # Agrupar por sede, cliente, mensajero y fecha
        data = data.groupby([
            'sede_id', 'cliente_id', 'fecha_solicitud', 'mensajero_id'
        ]).agg(
            total_finalizado_mensajero=('servicio_id', 'count')
        ).reset_index()
        
        data = pd.merge(data, dim_fecha[['date', 'key_dim_fecha']], left_on='fecha_solicitud', right_on='date', how='left')
        data.drop(columns=['date'], inplace=True)
        data.drop(columns=['fecha_solicitud'], inplace=True)
        data.rename(columns={'key_dim_fecha': 'id_fecha'}, inplace=True)
       
        print(f"Data grouped successfully. Records found: {len(data)}")
        return data

    except Exception as e:
        print(f"Error transforming data: {str(e)}")
        raise

        
def load(transformed, warehouse_db):
  try:
      
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
        
def run_etl_mensajeria_diario():
  #get databases connections
  warehouse_db = get_database_connections()
  
  # extract the data
  data = extract(warehouse_db)

  # transform data
  transformed = transform(data, warehouse_db)

  # load to the warehouse
  load(transformed, warehouse_db)

if __name__ == "__main__":
    run_etl_mensajeria_diario()