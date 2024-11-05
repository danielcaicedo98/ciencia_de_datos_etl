# src/etl_servicio.py

import pandas as pd
from sqlalchemy import create_engine
from datetime import timedelta
import yaml

def load_config(config_path='config.yml'):
    """Cargar el archivo de configuración (config.yml)"""
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config

def create_db_connection(config):
    """Crea la conexión a las bases de datos de origen y destino utilizando SQLAlchemy."""
    config_rf = config['fuente']
    config_etl = config['bodega']
    
    # Construir la URL de la base de datos de origen
    url_rf = (f"{config_rf['drivername']}://{config_rf['user']}:{config_rf['password']}@"
              f"{config_rf['host']}:{config_rf['port']}/{config_rf['dbname']}")
    
    # Construir la URL de la base de datos de destino (ETL)
    url_etl = (f"{config_etl['drivername']}://{config_etl['user']}:{config_etl['password']}@"
               f"{config_etl['host']}:{config_etl['port']}/{config_etl['dbname']}")
    
    # Crear las conexiones con SQLAlchemy
    ra_fu = create_engine(url_rf)  # Conexión a la base de datos de origen
    etl_conn = create_engine(url_etl)  # Conexión a la base de datos de destino
    
    return ra_fu, etl_conn

def load_tables(etl_conn):
    """Carga las tablas necesarias desde la base de datos."""
    dim_servicio = pd.read_sql_table('trans_servicio', etl_conn)
    dim_fecha = pd.read_sql_table('dim_fecha', etl_conn)
    dim_hora = pd.read_sql_table('dim_hora', etl_conn)
    return dim_servicio, dim_fecha, dim_hora

def merge_tables_with_fecha(dim_servicio, dim_fecha):
    """Realiza las uniones entre dim_servicio y dim_fecha para obtener claves de fecha."""
    # Unir con dim_fecha para las fechas
    for fecha_column in ['fecha_iniciado', 'fecha_asignado', 'fecha_recogido', 'fecha_entregado', 'fecha_terminado']:
        dim_servicio = pd.merge(
            dim_servicio, dim_fecha[['date', 'key_dim_fecha']], 
            left_on=fecha_column, right_on='date', how='left'
        )
        dim_servicio.drop(columns=['date'], inplace=True)
        dim_servicio.rename(columns={'key_dim_fecha': f'key_{fecha_column}'}, inplace=True)
    return dim_servicio

def merge_tables_with_hora(dim_servicio, dim_hora):
    """Realiza las uniones entre dim_servicio y dim_hora para obtener claves de hora."""
    for hora_column in ['hora_iniciado', 'hora_asignado', 'hora_recogido', 'hora_entregado', 'hora_terminado']:
        dim_servicio = pd.merge(
            dim_servicio, dim_hora[['time', 'key_dim_hora']], 
            left_on=hora_column, right_on='time', how='left'
        )
        dim_servicio.drop(columns=['time'], inplace=True)
        dim_servicio.rename(columns={'key_dim_hora': f'key_{hora_column}'}, inplace=True)
    return dim_servicio

def calculate_time_differences(dim_servicio):
    """Calcula las diferencias de tiempo entre las distintas etapas del servicio."""
    
    # Calcula los tiempos transcurridos entre eventos
    for start_col, end_col, time_col in [
        ('hora_iniciado', 'hora_asignado', 'tiempo_iniciado_asignado'),
        ('hora_asignado', 'hora_recogido', 'tiempo_asignado_recogido'),
        ('hora_recogido', 'hora_entregado', 'tiempo_recogido_entregado'),
        ('hora_entregado', 'hora_terminado', 'tiempo_entregado_terminado')
    ]:
        dim_servicio[start_col] = dim_servicio[start_col].apply(lambda x: timedelta(hours=x.hour, minutes=x.minute, seconds=x.second))
        dim_servicio[end_col] = dim_servicio[end_col].apply(lambda x: timedelta(hours=x.hour, minutes=x.minute, seconds=x.second))
        
        dim_servicio[f'fecha_hora_{start_col}'] = dim_servicio[f'fecha_{start_col}'] + dim_servicio[start_col]
        dim_servicio[f'fecha_hora_{end_col}'] = dim_servicio[f'fecha_{end_col}'] + dim_servicio[end_col]
        
        dim_servicio[f'espera_{start_col}_{end_col}'] = dim_servicio[f'fecha_hora_{end_col}'] - dim_servicio[f'fecha_hora_{start_col}']
        dim_servicio[time_col] = dim_servicio[f'espera_{start_col}_{end_col}'].dt.total_seconds()
    
    return dim_servicio

def filter_columns(dim_servicio):
    """Filtra las columnas necesarias para el proceso ETL y elimina las columnas vacías."""
    columnas_a_conservar = [
        "servicio_id", "key_fecha_iniciado", "key_fecha_asignado",
        "key_fecha_recogido", "key_fecha_entregado", "key_fecha_terminado",
        "key_hora_iniciado", "key_hora_asignado", "key_hora_recogido",
        "key_hora_entregado", "key_hora_terminado", "tiempo_iniciado_asignado",
        'tiempo_asignado_recogido', 'tiempo_recogido_entregado', 'tiempo_entregado_terminado'
    ]
    
    # Filtra las columnas y elimina las que son completamente nulas
    dim_servicio = dim_servicio[columnas_a_conservar].dropna(axis=1, how='all')
    
    return dim_servicio

def load_to_db(dim_servicio, etl_conn):
    """Carga los datos procesados en la base de datos de destino."""
    dim_servicio.to_sql('servicio_accumulating_snapshot', con=etl_conn, if_exists='replace', index=False)

def run_etl_servicio_acumulado(config_path='config.yml'):
    """Función principal que orquesta todo el proceso ETL para la tabla de servicio."""
    # Cargar configuración
    config = load_config(config_path)
    
    # Crear conexiones a las bases de datos
    ra_fu, etl_conn = create_db_connection(config)
    
    # Cargar las tablas necesarias
    dim_servicio, dim_fecha, dim_hora = load_tables(etl_conn)
    
    # Realizar las uniones con dim_fecha y dim_hora
    dim_servicio = merge_tables_with_fecha(dim_servicio, dim_fecha)
    dim_servicio = merge_tables_with_hora(dim_servicio, dim_hora)
    
    # Calcular las diferencias de tiempo
    dim_servicio = calculate_time_differences(dim_servicio)
    
    # Filtrar las columnas necesarias
    dim_servicio = filter_columns(dim_servicio)
    
    # Cargar los resultados en la base de datos de destino
    load_to_db(dim_servicio, etl_conn)
    
    print("ETL para 'servicio_accumulating_snapshot' completado exitosamente!")
