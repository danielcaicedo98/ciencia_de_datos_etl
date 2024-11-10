from datetime import date
import pandas as pd
import holidays
import yaml
from sqlalchemy import create_engine

def load_config(config_path='config.yml'):
    """Carga el archivo de configuración."""
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config

def create_db_connection(config):
    """Crea las conexiones a las bases de datos usando SQLAlchemy."""
    config_etl = config['bodega']
    
    # Construir la URL de la base de datos
    url_etl = (f"{config_etl['drivername']}://{config_etl['user']}:{config_etl['password']}@"
               f"{config_etl['host']}:{config_etl['port']}/{config_etl['dbname']}")
    
    # Crear la conexión con SQLAlchemy
    etl_conn = create_engine(url_etl)
    
    return etl_conn

def generate_dim_fecha():
    """Genera la dimensión 'fecha'."""
    # Crear un rango de fechas desde el 1 de enero de 2023 hasta el 1 de enero de 2025
    dim_fecha = pd.DataFrame({
        "date": pd.date_range(start='1/1/2023', end='1/1/2025', freq='D')
    })
    
    # Agregar columnas con información sobre las fechas
    dim_fecha["year"] = dim_fecha["date"].dt.year
    dim_fecha["month"] = dim_fecha["date"].dt.month
    dim_fecha["day"] = dim_fecha["date"].dt.day
    dim_fecha["weekday"] = dim_fecha["date"].dt.weekday
    dim_fecha["quarter"] = dim_fecha["date"].dt.quarter
    dim_fecha["day_of_year"] = dim_fecha["date"].dt.day_of_year
    dim_fecha["day_of_month"] = dim_fecha["date"].dt.days_in_month
    dim_fecha["month_str"] = dim_fecha["date"].dt.month_name()
    dim_fecha["day_str"] = dim_fecha["date"].dt.day_name()
    dim_fecha["date_str"] = dim_fecha["date"].dt.strftime("%d/%m/%Y")
    
    # Obtener los días festivos de Colombia
    co_holidays = holidays.CO(language="es")
    dim_fecha["is_Holiday"] = dim_fecha["date"].apply(lambda x: x in co_holidays)
    dim_fecha["holiday"] = dim_fecha["date"].apply(lambda x: co_holidays.get(x))
    
    # Agregar columnas para la fecha de guardado y los fines de semana
    dim_fecha["saved"] = date.today()
    dim_fecha["weekend"] = dim_fecha["weekday"].apply(lambda x: x > 4)
    
    return dim_fecha

def load_to_db(dim_fecha, engine):
    """Carga la dimensión 'fecha' en la base de datos."""
    dim_fecha.to_sql('dim_fecha', con=engine, if_exists='replace', index_label='key_dim_fecha')

def run_etl_dim_fecha(config_path='config.yml'):
    """Función principal para ejecutar el proceso ETL de la dimensión fecha."""
    # Cargar la configuración
    config = load_config(config_path)
    
    # Crear la conexión a la base de datos
    etl_conn = create_db_connection(config)
    
    # Generar la dimensión fecha
    dim_fecha = generate_dim_fecha()
    
    # Cargar la dimensión 'fecha' en la base de datos
    load_to_db(dim_fecha, etl_conn)
    
    print("ETL para 'dim_fecha' completado exitosamente!")
