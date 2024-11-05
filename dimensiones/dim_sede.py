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
    """Carga la tabla 'sede' desde la base de datos."""
    dim_sede = pd.read_sql_table("sede", ra_fu)
    return dim_sede

def process_data(dim_sede):
    """Procesa los datos para la dimensión 'sede'."""
    # Establecer 'sede_id' como índice
    dim_sede.set_index('sede_id', inplace=True)
    return dim_sede

def load_to_db(dim_sede, etl_conn):
    """Carga los datos procesados en la base de datos de destino."""
    dim_sede.to_sql('dim_sede', con=etl_conn, index_label='sede_id', if_exists='replace')

def run_etl_dim_sede(config_path='config.yml'):
    """Función principal para ejecutar el proceso ETL de la dimensión sede."""
    # Cargar la configuración
    config = load_config(config_path)
    
    # Crear las conexiones a las bases de datos
    ra_fu, etl_conn = create_db_connection(config)
    
    # Cargar los datos desde la base de datos
    dim_sede = load_data_from_db(ra_fu)
    
    # Procesar los datos
    dim_sede = process_data(dim_sede)
    
    # Cargar los datos procesados en la base de datos de destino
    load_to_db(dim_sede, etl_conn)
    
    print("ETL para 'dim_sede' completado exitosamente!")
