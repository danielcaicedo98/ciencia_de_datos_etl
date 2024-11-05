from datetime import  timedelta
import pandas as pd
from sqlalchemy import text
from dimensiones.db_connection import get_database_connections

def extract(source_db):
  try:
    # Primero, fusionamos df_agrupado con dim_fecha para obtener las claves de fecha
    dim_servicio = pd.read_sql_table('trans_servicio', source_db)
    dim_fecha = pd.read_sql_table('dim_fecha', source_db)
    dim_hora = pd.read_sql_table('dim_hora', source_db)

    # Merge para cada fecha que necesites
    dim_servicio = pd.merge(dim_servicio, dim_fecha[['date', 'key_dim_fecha']], left_on='fecha_iniciado', right_on='date', how='left')
    dim_servicio.drop(columns=['date'], inplace=True)
    dim_servicio.rename(columns={'key_dim_fecha': 'key_fecha_iniciado'}, inplace=True)

    dim_servicio = pd.merge(dim_servicio, dim_fecha[['date', 'key_dim_fecha']], left_on='fecha_asignado', right_on='date', how='left')
    dim_servicio.drop(columns=['date'], inplace=True)
    dim_servicio.rename(columns={'key_dim_fecha': 'key_fecha_asignado'}, inplace=True)

    dim_servicio = pd.merge(dim_servicio, dim_fecha[['date', 'key_dim_fecha']], left_on='fecha_recogido', right_on='date', how='left')
    dim_servicio.drop(columns=['date'], inplace=True)
    dim_servicio.rename(columns={'key_dim_fecha': 'key_fecha_recogido'}, inplace=True)

    dim_servicio = pd.merge(dim_servicio, dim_fecha[['date', 'key_dim_fecha']], left_on='fecha_entregado', right_on='date', how='left')
    dim_servicio.drop(columns=['date'], inplace=True)
    dim_servicio.rename(columns={'key_dim_fecha': 'key_fecha_entregado'}, inplace=True)

    dim_servicio = pd.merge(dim_servicio, dim_fecha[['date', 'key_dim_fecha']], left_on='fecha_terminado', right_on='date', how='left')
    dim_servicio.drop(columns=['date'], inplace=True)
    dim_servicio.rename(columns={'key_dim_fecha': 'key_fecha_terminado'}, inplace=True)


    # LLAVES PRIMARIAS A DIMENSION HORA

    dim_servicio = pd.merge(dim_servicio, dim_hora[['time', 'key_dim_hora']], left_on='hora_iniciado', right_on='time', how='left')
    dim_servicio.drop(columns=['time'], inplace=True)
    dim_servicio.rename(columns={'key_dim_hora': 'key_hora_iniciado'}, inplace=True)

    dim_servicio = pd.merge(dim_servicio, dim_hora[['time', 'key_dim_hora']], left_on='hora_asignado', right_on='time', how='left')
    dim_servicio.drop(columns=['time'], inplace=True)
    dim_servicio.rename(columns={'key_dim_hora': 'key_hora_asignado'}, inplace=True)

    dim_servicio = pd.merge(dim_servicio, dim_hora[['time', 'key_dim_hora']], left_on='hora_recogido', right_on='time', how='left')
    dim_servicio.drop(columns=['time'], inplace=True)
    dim_servicio.rename(columns={'key_dim_hora': 'key_hora_recogido'}, inplace=True)

    dim_servicio = pd.merge(dim_servicio, dim_hora[['time', 'key_dim_hora']], left_on='hora_entregado', right_on='time', how='left')
    dim_servicio.drop(columns=['time'], inplace=True)
    dim_servicio.rename(columns={'key_dim_hora': 'key_hora_entregado'}, inplace=True)

    dim_servicio = pd.merge(dim_servicio, dim_hora[['time', 'key_dim_hora']], left_on='hora_terminado', right_on='time', how='left')
    dim_servicio.drop(columns=['time'], inplace=True)
    dim_servicio.rename(columns={'key_dim_hora': 'key_hora_terminado'}, inplace=True)
    return dim_servicio
  
  except Exception as e:
    print(f"Error extracting data: {str(e)}")

def transform(dim_servicio):
  try:
    # TIEMPO TRANSCURRIDO ENTRE INICIADO Y ASIGNADO

    dim_servicio['hora_asignado'] = dim_servicio['hora_asignado'].apply(lambda x: timedelta(hours=x.hour, minutes=x.minute, seconds=x.second))
    dim_servicio['hora_iniciado'] = dim_servicio['hora_iniciado'].apply(lambda x: timedelta(hours=x.hour, minutes=x.minute, seconds=x.second))

    dim_servicio['fecha_hora_iniciado'] = dim_servicio['fecha_iniciado'] + dim_servicio['hora_iniciado']
    dim_servicio['fecha_hora_asignado'] = dim_servicio['fecha_asignado'] + dim_servicio['hora_asignado'] 
    dim_servicio['espera_iniciado_asignado'] = dim_servicio['fecha_hora_asignado'] - dim_servicio['fecha_hora_iniciado']
    dim_servicio['tiempo_iniciado_asignado'] = dim_servicio['espera_iniciado_asignado'].dt.total_seconds()



    # TIEMPO TRANSCURRIDO ENTRE ASIGNADO Y RECOGIDO

    dim_servicio['hora_recogido'] = dim_servicio['hora_recogido'].apply(lambda x: timedelta(hours=x.hour, minutes=x.minute, seconds=x.second))

    dim_servicio['fecha_hora_recogido'] = dim_servicio['fecha_recogido'] + dim_servicio['hora_recogido'] 
    dim_servicio['espera_asignado_recogido'] = dim_servicio['fecha_hora_recogido'] - dim_servicio['fecha_hora_asignado']
    dim_servicio['tiempo_asignado_recogido'] = dim_servicio['espera_asignado_recogido'].dt.total_seconds()


    # TIEMPO TRANSCURRIDO ENTRE RECOGIDO Y ENTREGADO

    dim_servicio['hora_entregado'] = dim_servicio['hora_entregado'].apply(lambda x: timedelta(hours=x.hour, minutes=x.minute, seconds=x.second))

    dim_servicio['fecha_hora_entregado'] = dim_servicio['fecha_entregado'] + dim_servicio['hora_entregado'] 
    dim_servicio['espera_recogido_entregado'] = dim_servicio['fecha_hora_entregado'] - dim_servicio['fecha_hora_recogido']
    dim_servicio['tiempo_recogido_entregado'] = dim_servicio['espera_recogido_entregado'].dt.total_seconds()

    # TIEMPO TRANSCURRIDO ENTRE RECOGIDO Y ENTREGADO

    dim_servicio['hora_terminado'] = dim_servicio['hora_terminado'].apply(lambda x: timedelta(hours=x.hour, minutes=x.minute, seconds=x.second))

    dim_servicio['fecha_hora_terminado'] = dim_servicio['fecha_terminado'] + dim_servicio['hora_terminado'] 
    dim_servicio['espera_entregado_terminado'] = dim_servicio['fecha_hora_terminado'] - dim_servicio['fecha_hora_entregado']
    dim_servicio['tiempo_entregado_terminado'] = dim_servicio['espera_entregado_terminado'].dt.total_seconds()
    return dim_servicio
  
  except Exception as e:
    print(f"Error transforming data: {str(e)}")


def load(warehouse_db, dim_servicio):
  try:
    columnas_a_conservar = [
        "servicio_id", "key_fecha_iniciado", "key_fecha_asignado",
        "key_fecha_recogido", "key_fecha_entregado", "key_fecha_terminado",
        "key_hora_iniciado", "key_hora_asignado", "key_hora_recogido",
        "key_hora_entregado", "key_hora_terminado","tiempo_iniciado_asignado",
        'tiempo_asignado_recogido','tiempo_recogido_entregado','tiempo_entregado_terminado'
    ]

    # Filtramos las columnas, asegurándonos de que existan
    dim_servicio = dim_servicio[columnas_a_conservar].dropna(axis=1, how='all')

    dim_servicio.to_sql('servicio_accumulating_snapshot', con=warehouse_db, if_exists='replace', index=False)
  except Exception as e:
    print(f"Error loading data: {str(e)}")

def run_etl_servicio_acumulado():
  #get databases connections
  source_db, warehouse_db = get_database_connections()
  
  # extract the data
  data = extract(warehouse_db)

  # transform data
  transformed = transform(data)

  # load to the warehouse
  load(warehouse_db, transformed)

if __name__ == "__main__":
    run_etl_servicio_acumulado()