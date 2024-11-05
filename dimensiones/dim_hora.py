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
    config_etl = config['bodega']
    
    # Construir la URL de la base de datos
    url_etl = (f"{config_etl['drivername']}://{config_etl['user']}:{config_etl['password']}@"
               f"{config_etl['host']}:{config_etl['port']}/{config_etl['dbname']}")
    
    # Crear la conexión con SQLAlchemy
    etl_conn = create_engine(url_etl)
    
    return etl_conn

def generate_dim_hora():
    """Genera la dimensión 'hora' con las columnas necesarias."""
    # Crear un rango de tiempo para todo el día, con minutos y segundos
    dim_hora = pd.DataFrame({
        "time": pd.date_range(start='00:00:00', end='23:00:00', freq='h').time  # Extraer solo la parte de la hora
    })

    # Extraer atributos del tiempo
    dim_hora["hour_24"] = pd.Series([t.hour for t in dim_hora["time"]])  # Hora en formato 24 horas
    # dim_hora["minute"] = pd.Series([t.minute for t in dim_hora["time"]])  # Minutos
    # dim_hora["second"] = pd.Series([t.second for t in dim_hora["time"]])  # Segundos
    dim_hora["period"] = pd.Series([t.strftime('%p') for t in dim_hora["time"]])  # AM/PM
    dim_hora["hour_str"] = pd.Series([t.strftime('%I:%M:%S %p') for t in dim_hora["time"]])  # Hora en formato legible (incluye segundos)

    return dim_hora

def load_to_db(dim_hora, engine):
    """Carga la dimensión 'hora' en la base de datos."""
    dim_hora.to_sql('dim_hora', con=engine, if_exists='replace', index_label='key_dim_hora')

def run_etl_dim_hora(config_path='config.yml'):
    """Función principal para ejecutar el proceso ETL de la dimensión hora."""
    # Cargar la configuración
    config = load_config(config_path)
    
    # Crear la conexión a la base de datos
    etl_conn = create_db_connection(config)
    
    # Generar la dimensión hora
    dim_hora = generate_dim_hora()
    
    # Cargar la dimensión 'hora' en la base de datos
    load_to_db(dim_hora, etl_conn)
    
    print("ETL para 'dim_hora' completado exitosamente!")
    
if __name__ == "__main__":
    run_etl_dim_hora()
