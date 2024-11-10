from datetime import timedelta
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
    url_etl = (f"{config_etl['drivername']}://{config_etl['user']}:{config_etl['password']}@"
               f"{config_etl['host']}:{config_etl['port']}/{config_etl['dbname']}")
    etl_conn = create_engine(url_etl)
    return etl_conn

def load_processed_data(etl_conn):
    """Carga los datos procesados de la tabla trans_servicio en la base de datos."""
    df_trans_servicio = pd.read_sql_table('trans_servicio', etl_conn)
    dim_fecha = pd.read_sql_table('dim_fecha', etl_conn)
    dim_hora = pd.read_sql_table('dim_hora', etl_conn)
    return df_trans_servicio,dim_fecha,dim_hora

def process_hourly_service_counts(df_trans_servicio, dim_fecha, dim_hora):
    """Procesa los datos para contar los servicios activos por hora y enlazarlos con dim_fecha y dim_hora."""
    
    # Asegurarse de que 'fecha_asignado' y 'fecha_entregado' son de tipo datetime
    df_trans_servicio['fecha_asignado'] = pd.to_datetime(df_trans_servicio['fecha_asignado'])
    df_trans_servicio['fecha_entregado'] = pd.to_datetime(df_trans_servicio['fecha_entregado'])
    
    # Convertir 'hora_asignado' y 'hora_entregado' a timedelta para sumarlas a la fecha
    df_trans_servicio['hora_asignado'] = df_trans_servicio['hora_asignado'].apply(lambda x: timedelta(hours=x.hour, minutes=x.minute, seconds=x.second))
    df_trans_servicio['hora_entregado'] = df_trans_servicio['hora_entregado'].apply(lambda x: timedelta(hours=x.hour, minutes=x.minute, seconds=x.second))
    
    # Sumar fecha y hora para obtener datetime completo
    df_trans_servicio['fecha_hora_asignado'] = (df_trans_servicio['fecha_asignado'] + df_trans_servicio['hora_asignado']).dt.floor('h')
    df_trans_servicio['fecha_hora_entregado'] = (df_trans_servicio['fecha_entregado'] + df_trans_servicio['hora_entregado']).dt.floor('h')
    
    # Crear una lista de intervalos horarios en el rango de fechas
    all_intervals = []
    for _, row in df_trans_servicio.iterrows():
        current_time = row['fecha_hora_asignado']
        while current_time <= row['fecha_hora_entregado']:
            all_intervals.append(current_time)
            current_time += timedelta(hours=1)
    
    # Crear un DataFrame para el conteo por hora
    df_intervals = pd.DataFrame(all_intervals, columns=['fecha_hora'])
    df_hourly_counts = df_intervals.value_counts().reset_index(name='conteo_servicios')
    df_hourly_counts.columns = ['fecha_hora', 'conteo_servicios']
    
    # Separar fecha y hora en columnas diferentes
    df_hourly_counts['fecha'] = df_hourly_counts['fecha_hora'].dt.date
    df_hourly_counts['fecha'] = pd.to_datetime(df_hourly_counts['fecha'].astype(str) + ' 00:00:00')
    df_hourly_counts['hora'] = df_hourly_counts['fecha_hora'].dt.time
    
    # Ordenar por fecha y hora de forma ascendente
    df_hourly_counts = df_hourly_counts.sort_values(by=['fecha', 'hora']).reset_index(drop=True)
    
    # Agregar columna id
    df_hourly_counts['id'] = pd.RangeIndex(start=1, stop=len(df_hourly_counts) + 1, step=1)
    
    df_hourly_counts = pd.merge(df_hourly_counts, dim_fecha[['date', 'key_dim_fecha']], left_on='fecha', right_on='date', how='left')
    df_hourly_counts.drop(columns=['date'], inplace=True)
    
    df_hourly_counts = pd.merge(df_hourly_counts, dim_hora[['time', 'key_dim_hora']], left_on='hora', right_on='time', how='left')
    df_hourly_counts.drop(columns=['time'], inplace=True)
    
    # Reorganizar columnas
    df_hourly_counts = df_hourly_counts[['id' , 'key_dim_fecha', 'key_dim_hora','fecha', 'hora','conteo_servicios']]
    
    return df_hourly_counts

def load_to_db(df_hourly_counts, etl_conn):
    """Carga el DataFrame de conteo horario en la base de datos de destino."""
    df_hourly_counts.to_sql('conteo_servicios_por_hora', etl_conn, if_exists='replace', index=False)

def run_etl_servicio_mensajeria_hora(config_path='config.yml'):
    """Función principal para ejecutar el proceso ETL del conteo de servicios por hora."""
    # Cargar la configuración
    print("CARGANDO TRANSFORMACIÓN DE CONTEO DE SERVICIOS POR HORA...")
    config = load_config(config_path)
    
    # Crear la conexión a la base de datos de destino
    etl_conn = create_db_connection(config)
    
    # Cargar los datos procesados de trans_servicio
    df_trans_servicio,dim_fecha,dim_hora = load_processed_data(etl_conn)
    
    # Procesar los datos para obtener el conteo horario
    df_hourly_counts = process_hourly_service_counts(df_trans_servicio,dim_fecha,dim_hora)
    
    # Cargar el conteo en la base de datos de destino
    load_to_db(df_hourly_counts, etl_conn)
    
    print("ETL para 'conteo_servicios_por_hora' completado exitosamente!")

if __name__ == "__main__":
    run_etl_servicio_mensajeria_hora()
