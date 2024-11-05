import datetime
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
    
    # Construir las URLs de las bases de datos
    url_rf = (f"{config_rf['drivername']}://{config_rf['user']}:{config_rf['password']}@"
              f"{config_rf['host']}:{config_rf['port']}/{config_rf['dbname']}")
    url_etl = (f"{config_etl['drivername']}://{config_etl['user']}:{config_etl['password']}@"
               f"{config_etl['host']}:{config_etl['port']}/{config_etl['dbname']}")
    
    # Crear las conexiones con SQLAlchemy
    ra_fu = create_engine(url_rf)
    etl_conn = create_engine(url_etl)
    
    return ra_fu, etl_conn

def load_data_from_db(engine):
    """Carga las tablas necesarias desde la base de datos."""
    df_cliente = pd.read_sql_table("cliente", engine)
    df_t_cliente = pd.read_sql_table("tipo_cliente", engine)
    df_t_cliente.rename(columns={'nombre': 'tipo_cliente'}, inplace=True)
    
    return df_cliente, df_t_cliente

def transform_data(df_cliente, df_t_cliente):
    """Transforma los datos de las tablas cargadas."""
    # Merge dataframes
    dim_cliente = pd.merge(df_cliente, df_t_cliente, left_on='tipo_cliente_id', 
                           right_on='tipo_cliente_id', how='inner')
    
    # Selección de columnas a conservar
    columnas_a_conservar = [
        "cliente_id", "nit_cliente", "nombre", "email", "direccion", "sector",
        "nombre_contacto", "telefono", "ciudad_id", "tipo_cliente", "activo"
    ]
    dim_cliente = dim_cliente[columnas_a_conservar].dropna(axis=1, how='all')
    
    # Establecer 'cliente_id' como índice
    dim_cliente.set_index('cliente_id', inplace=True)
    
    return dim_cliente

def load_to_db(dim_cliente, engine):
    """Carga los datos transformados en la base de datos."""
    dim_cliente.to_sql('dim_cliente', con=engine, index_label='cliente_id', if_exists='replace')

def run_etl_dim_cliente(config_path='config.yml'):
    """Función principal para ejecutar el proceso ETL completo de cliente."""
    # Cargar la configuración
    config = load_config(config_path)
    
    # Crear las conexiones a las bases de datos
    ra_fu, etl_conn = create_db_connection(config)
    
    # Cargar los datos
    df_cliente, df_t_cliente = load_data_from_db(ra_fu)
    
    # Transformar los datos
    dim_cliente = transform_data(df_cliente, df_t_cliente)
    
    # Cargar los datos transformados a la base de datos de destino
    load_to_db(dim_cliente, etl_conn)
    
    print("ETL para 'cliente' completado exitosamente!")