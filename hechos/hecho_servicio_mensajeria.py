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


# Primero, fusionamos 
dim_ciudad = pd.read_sql_table('dim_ciudad', etl_conn)
dim_cliente = pd.read_sql_table('dim_cliente', etl_conn)
dim_mensajero = pd.read_sql_table('dim_mensajero', etl_conn)
dim_sede = pd.read_sql_table('dim_sede', etl_conn)
dim_type_service = pd.read_sql_table('dim_type_service', etl_conn)
dim_fecha = pd.read_sql_table('dim_fecha', etl_conn)
dim_hora = pd.read_sql_table('dim_hora', etl_conn)
