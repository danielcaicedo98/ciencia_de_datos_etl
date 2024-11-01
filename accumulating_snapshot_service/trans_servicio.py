import datetime
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

df_estado_servicio = pd.read_sql_table('mensajeria_estadosservicio', ra_fu)

df_columns = [
    "id_servicio", "hora_iniciado", "hora_asignado", 
    "hora_recogido", "hora_en_destino", "hora_entregado", 
    "fecha_iniciado", "fecha_asignado", "fecha_recogido", 
    "fecha_en_destino", "fecha_entregado"
]


df_agrupado = (df_estado_servicio
               .groupby('servicio_id')
               .agg(
                   hora_iniciado=('hora', lambda x: x[df_estado_servicio['estado_id'] == 1].values[0] if not x[df_estado_servicio['estado_id'] == 1].empty else None),
                   hora_asignado=('hora', lambda x: x[df_estado_servicio['estado_id'] == 2].values[0] if not x[df_estado_servicio['estado_id'] == 2].empty else None),
                   hora_recogido=('hora', lambda x: x[df_estado_servicio['estado_id'] == 4].values[0] if not x[df_estado_servicio['estado_id'] == 4].empty else None),
                   hora_entregado=('hora', lambda x: x[df_estado_servicio['estado_id'] == 5].values[0] if not x[df_estado_servicio['estado_id'] == 5].empty else None),
                   hora_terminado=('hora', lambda x: x[df_estado_servicio['estado_id'] == 6].values[0] if not x[df_estado_servicio['estado_id'] == 6].empty else None),

                   fecha_iniciado=('fecha', lambda x: x[df_estado_servicio['estado_id'] == 1].values[0] if not x[df_estado_servicio['estado_id'] == 1].empty else None),
                   fecha_asignado=('fecha', lambda x: x[df_estado_servicio['estado_id'] == 2].values[0] if not x[df_estado_servicio['estado_id'] == 2].empty else None),
                   fecha_recogido=('fecha', lambda x: x[df_estado_servicio['estado_id'] == 4].values[0] if not x[df_estado_servicio['estado_id'] == 4].empty else None),
                   fecha_entregado=('fecha', lambda x: x[df_estado_servicio['estado_id'] == 5].values[0] if not x[df_estado_servicio['estado_id'] == 5].empty else None),
                   fecha_terminado=('fecha', lambda x: x[df_estado_servicio['estado_id'] == 6].values[0] if not x[df_estado_servicio['estado_id'] == 6].empty else None)
               )
               .reset_index())


df_agrupado.columns.name = None  
df_agrupado = df_agrupado.dropna()
df_agrupado.to_sql('trans_servicio',etl_conn,if_exists='replace',index_label='key_servicio')