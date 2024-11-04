import datetime
from datetime import  date
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

#loading table
dim_cliente = pd.read_sql_table("cliente",etl_conn)


dim_cliente["saved"] = date.today()
dim_cliente.reset_index(drop=True, inplace=True)
dim_cliente.head()
dim_cliente.info()

dim_cliente.tosql('dim_cliente', con=etl_conn, index_label='key_dim_cliente',if_exists='replace')