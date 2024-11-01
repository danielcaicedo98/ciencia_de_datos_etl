import datetime
from datetime import  timedelta
import numpy as np
import pandas as pd
import yaml
from sqlalchemy import create_engine

with open('../config.yml', 'r') as f:
    config = yaml.safe_load(f)   
    config_rf = config['fuente'] 
    config_etl = config['bodega']

# Construct the database URL
url_rf = (f"{config_rf['drivername']}://{config_rf['user']}:{config_rf['password']}@{config_rf['host']}:"
          f"{config_rf['port']}/{config_rf['dbname']}")
url_etl = (f"{config_etl['drivername']}://{config_etl['user']}:{config_etl['password']}@{config_etl['host']}:"
           f"{config_etl['port']}/{config_etl['dbname']}")
# Create the SQLAlchemy Engine
ra_fu = create_engine(url_rf)
etl_conn = create_engine(url_etl)


# Primero, fusionamos df_agrupado con dim_fecha para obtener las claves de fecha
dim_servicio = pd.read_sql_table('trans_servicio', etl_conn)
dim_fecha = pd.read_sql_table('dim_fecha', etl_conn)
dim_hora = pd.read_sql_table('dim_hora', etl_conn)

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



columnas_a_conservar = [
    "servicio_id", "key_fecha_iniciado", "key_fecha_asignado",
    "key_fecha_recogido", "key_fecha_entregado", "key_fecha_terminado",
    "key_hora_iniciado", "key_hora_asignado", "key_hora_recogido",
    "key_hora_entregado", "key_hora_terminado","tiempo_iniciado_asignado",
    'tiempo_asignado_recogido','tiempo_recogido_entregado','tiempo_entregado_terminado'
]

# Filtramos las columnas, asegurándonos de que existan
dim_servicio = dim_servicio[columnas_a_conservar].dropna(axis=1, how='all')

dim_servicio.to_sql('servicio_accumulating_snapshot', con=etl_conn, if_exists='replace', index=False)