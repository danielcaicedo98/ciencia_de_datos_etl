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
    """Carga los datos desde la base de datos de la fuente (mensajeria_estadosservicio)."""
    df_estado_servicio = pd.read_sql_table('mensajeria_estadosservicio', ra_fu)
    return df_estado_servicio

def process_data(df_estado_servicio):
    """Procesa los datos para obtener el estado del servicio."""
    # Agrupar los datos y calcular las columnas según el estado_id
    df_agrupado = (df_estado_servicio
                   .groupby('servicio_id')
                   .agg(
                       hora_iniciado=('hora', lambda x: x[df_estado_servicio['estado_id'] == 1].values[0] if not x[df_estado_servicio['estado_id'] == 1].empty else None),
                       hora_asignado=('hora', lambda x: x[df_estado_servicio['estado_id'] == 2].values[0] if not x[df_estado_servicio['estado_id'] == 2].empty else None),
                       hora_recogido=('hora', lambda x: x[df_estado_servicio['estado_id'] == 4].values[0] if not x[df_estado_servicio['estado_id'] == 4].empty else None),
                       hora_entregado=('hora', lambda x: x[df_estado_servicio['estado_id'] == 5].values[0] if not x[df_estado_servicio['estado_id'] == 5].empty else None),
                       hora_terminado=('hora', lambda x: x[df_estado_servicio['estado_id'] == 6].values[0] if not x[df_estado_servicio['estado_id'] == 6].empty else None),

                       fecha_iniciado=('fecha', lambda x: x[df_estado_servicio['estado_id'] == 1].values[0] if not x[df_estado_servicio['estado_id'] == 1].empty else None),
                       fecha_asignado=('fecha', lambda x: x[df_estado_servicio['estado_id'] == 2].values[0] if not x[df_estado_servicio['estado_id'] == 2].empty else None),
                       fecha_recogido=('fecha', lambda x: x[df_estado_servicio['estado_id'] == 4].values[0] if not x[df_estado_servicio['estado_id'] == 4].empty else None),
                       fecha_entregado=('fecha', lambda x: x[df_estado_servicio['estado_id'] == 5].values[0] if not x[df_estado_servicio['estado_id'] == 5].empty else None),
                       fecha_terminado=('fecha', lambda x: x[df_estado_servicio['estado_id'] == 6].values[0] if not x[df_estado_servicio['estado_id'] == 6].empty else None)
                   )
                   .reset_index())
    
    df_agrupado.columns.name = None  # Quitar el nombre de las columnas
    df_agrupado = df_agrupado.dropna()  # Eliminar las filas con valores nulos
    return df_agrupado

def load_to_db(df_agrupado, etl_conn):
    """Carga el DataFrame procesado en la base de datos de destino."""
    df_agrupado.to_sql('trans_servicio', etl_conn, if_exists='replace', index_label='key_servicio')

def run_etl_trans_servicio(config_path='config.yml'):
    """Función principal para ejecutar el proceso ETL de la dimensión servicio."""
    # Cargar la configuración
    print("CARGANDO TRANSFORMACIÓN DE SERVICIO.............................")
    config = load_config(config_path)
    
    # Crear las conexiones a las bases de datos
    ra_fu, etl_conn = create_db_connection(config)
    
    # Cargar los datos desde la base de datos
    df_estado_servicio = load_data_from_db(ra_fu)
    
    # Procesar los datos
    df_agrupado = process_data(df_estado_servicio)
    
    # Cargar los datos procesados en la base de datos de destino
    load_to_db(df_agrupado, etl_conn)
    
    print("ETL para 'trans_servicio' completado exitosamente!")
