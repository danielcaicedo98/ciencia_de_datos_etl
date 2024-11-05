import pandas as pd
from sqlalchemy import create_engine
import yaml

def load_config(config_path='config.yml'):
    """Carga el archivo de configuración desde el archivo config.yml."""
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config

def create_db_connection(config):
    """Crea la conexión a la base de datos utilizando SQLAlchemy."""
    config_rf = config['fuente']
    config_etl = config['bodega']
    
    # Construir la URL de la base de datos de la fuente
    url_rf = (f"{config_rf['drivername']}://{config_rf['user']}:{config_rf['password']}@"
              f"{config_rf['host']}:{config_rf['port']}/{config_rf['dbname']}")
    
    # Construir la URL de la base de datos de destino (ETL)
    url_etl = (f"{config_etl['drivername']}://{config_etl['user']}:{config_etl['password']}@"
               f"{config_etl['host']}:{config_etl['port']}/{config_etl['dbname']}")
    
    # Crear las conexiones con SQLAlchemy
    ra_fu = create_engine(url_rf)  # Fuente
    etl_conn = create_engine(url_etl)  # Base de datos de destino
    
    return ra_fu, etl_conn

def load_data_from_db(ra_fu):
    """Carga las tablas necesarias desde la base de datos."""
    cli_mens = pd.read_sql_table("clientes_mensajeroaquitoy", ra_fu)
    auth_user = pd.read_sql_table("auth_user", ra_fu)
    return cli_mens, auth_user

def process_data(cli_mens, auth_user):
    """Procesa los datos para crear la dimensión mensajero."""
    # Renombrar columnas y fusionar tablas
    cli_mens.rename(columns={'id': 'id_mensajero'}, inplace=True)
    dim_mensajero = pd.merge(cli_mens, auth_user, left_on='user_id', right_on='id', how='inner')
    dim_mensajero['nombre'] = dim_mensajero['first_name'] + ' ' + dim_mensajero['last_name']
    
    # Filtrar las columnas relevantes
    columnas_a_conservar = [
        "id_mensajero", "user_id", "nombre", "fecha_entrada", "fecha_salida",
        "salario", "telefono", "ciudad_operacion_id", "activo"
    ]
    dim_mensajero = dim_mensajero[columnas_a_conservar].dropna(axis=1, how='all')
    
    # Establecer 'id_mensajero' como índice
    dim_mensajero.set_index('id_mensajero', inplace=True)
    
    return dim_mensajero

def load_to_db(dim_mensajero, etl_conn):
    """Carga los datos procesados en la base de datos de destino."""
    dim_mensajero.to_sql('dim_mensajero', con=etl_conn, index_label='id_mensajero', if_exists='replace')

def run_etl_dim_mensajero(config_path='config.yml'):
    """Función principal para ejecutar el proceso ETL de la dimensión mensajero."""
    # Cargar la configuración
    config = load_config(config_path)
    
    # Crear las conexiones a las bases de datos
    ra_fu, etl_conn = create_db_connection(config)
    
    # Cargar los datos desde la base de datos
    cli_mens, auth_user = load_data_from_db(ra_fu)
    
    # Procesar los datos
    dim_mensajero = process_data(cli_mens, auth_user)
    
    # Cargar los datos procesados en la base de datos de destino
    load_to_db(dim_mensajero, etl_conn)
    
    print("ETL para 'dim_mensajero' completado exitosamente!")
