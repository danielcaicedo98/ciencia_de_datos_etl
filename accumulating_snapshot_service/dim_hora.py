import pandas as pd
from sqlalchemy import create_engine
import yaml

# Crear un rango de tiempo para todo el día, con minutos y segundos
dim_hora = pd.DataFrame({
    "time": pd.date_range(start='00:00:00', end='23:59:59', freq='S').time  # Extraer solo la parte de la hora
})

# Extraer atributos del tiempo

dim_hora["hour_24"] = pd.Series([t.hour for t in dim_hora["time"]])  # Hora en formato 24 horas
dim_hora["minute"] = pd.Series([t.minute for t in dim_hora["time"]])  # Minutos
dim_hora["second"] = pd.Series([t.second for t in dim_hora["time"]])  # Segundos
dim_hora["period"] = pd.Series([t.strftime('%p') for t in dim_hora["time"]])  # AM/PM
dim_hora["hour_str"] = pd.Series([t.strftime('%I:%M:%S %p') for t in dim_hora["time"]])  # Hora en formato legible (incluye segundos)


with open('../config.yml', 'r') as f:
    config = yaml.safe_load(f)    
    config_etl = config['bodega']

# Construct the database URL
url_etl = (f"{config_etl['drivername']}://{config_etl['user']}:{config_etl['password']}@{config_etl['host']}:"
           f"{config_etl['port']}/{config_etl['dbname']}")
# Create the SQLAlchemy Engine
etl_conn = create_engine(url_etl)

# Guardar en la base de datos
dim_hora.to_sql('dim_hora', etl_conn, if_exists='replace', index_label='key_dim_hora')
