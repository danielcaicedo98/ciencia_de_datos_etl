# etl.py

from datetime import date
import pandas as pd
import yaml
from sqlalchemy import create_engine

def load_config(config_path='config.yml'):
    """Carga el archivo de configuración."""
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config

def create_db_connection(config):
    """Crea las conexiones a las bases de datos usando SQLAlchemy."""
    config_rf = config['fuente']
    config_etl = config['bodega']
    
    # Construct the database URLs
    url_rf = (f"{config_rf['drivername']}://{config_rf['user']}:{config_rf['password']}@"
              f"{config_rf['host']}:{config_rf['port']}/{config_rf['dbname']}")
    url_etl = (f"{config_etl['drivername']}://{config_etl['user']}:{config_etl['password']}@"
               f"{config_etl['host']}:{config_etl['port']}/{config_etl['dbname']}")
    
    # Create SQLAlchemy Engine
    ra_fu = create_engine(url_rf)
    etl_conn = create_engine(url_etl)
    
    return ra_fu, etl_conn

def load_data_from_db(engine):
    """Carga las tablas necesarias desde la base de datos."""
    df_ciudad = pd.read_sql_table("ciudad", engine)
    df_ciudad.rename(columns={'departamento_id': 'id_departamento', 'nombre': 'ciudad'}, inplace=True)
    
    df_departamento = pd.read_sql_table("departamento", engine)
    df_departamento.rename(columns={'nombre': 'departamento'}, inplace=True)
    
    return df_ciudad, df_departamento

def transform_data(df_ciudad, df_departamento):
    """Transforma los datos de las tablas cargadas."""
    # Merge dataframes
    dim_ciudad = pd.merge(df_ciudad, df_departamento, left_on='id_departamento', 
                          right_on='departamento_id', how='inner')
    
    # Selección de columnas a conservar
    columnas_a_conservar = ["ciudad_id", "ciudad", "departamento"]
    dim_ciudad = dim_ciudad[columnas_a_conservar].dropna(axis=1, how='all')
    
    # Establecer 'ciudad_id' como índice
    dim_ciudad.set_index('ciudad_id', inplace=True)
    
    return dim_ciudad

def load_to_db(dim_ciudad, engine):
    """Carga los datos transformados en la base de datos."""
    dim_ciudad.to_sql('dim_ciudad', con=engine, index_label='ciudad_id', if_exists='replace')

def run_etl_dim_ciudad(config_path='config.yml'):
    """Función principal para ejecutar el proceso ETL completo."""
    # Cargar la configuración
    config = load_config(config_path)
    
    # Crear las conexiones a las bases de datos
    ra_fu, etl_conn = create_db_connection(config)
    
    # Cargar los datos
    df_ciudad, df_departamento = load_data_from_db(ra_fu)
    
    # Transformar los datos
    dim_ciudad = transform_data(df_ciudad, df_departamento)
    
    # Cargar los datos transformados a la base de datos de destino
    load_to_db(dim_ciudad, etl_conn)
    
    print("ETL para 'ciudad' completado exitosamente!")

