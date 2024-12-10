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
    servicio_estado = pd.read_sql_table('mensajeria_estadosservicio', ra_fu)
    servicio = pd.read_sql_table('mensajeria_servicio', ra_fu)
    usuario = pd.read_sql_table('clientes_usuarioaquitoy', ra_fu)
    
    
    usuario.rename(columns={'id': 'id_usuario'}, inplace=True) 
    
    columnas_usuario = [
        'id_usuario', 'sede_id'
    ]    
    usuario = usuario[columnas_usuario].dropna(axis=1, how='all') 
    
       
    servicio.rename(columns={'id': 'id_servicio'}, inplace=True)
    
    df_estado_servicio = pd.merge(servicio_estado, servicio, left_on='servicio_id', right_on='id_servicio', how='left')
    df_estado_servicio.drop(columns=['id_servicio'], inplace=True)
    
    df_estado_servicio = pd.merge(df_estado_servicio, usuario, left_on='usuario_id', right_on='id_usuario', how='left')
    df_estado_servicio.drop(columns=['id_usuario'], inplace=True)
    
    columnas_a_conservar = [
        "id", "fecha", "hora", "estado_id",
        "servicio_id", "cliente_id", "mensajero_id","sede_id"        
    ]    
    # Filtramos las columnas, asegurándonos de que existan
    df_estado_servicio = df_estado_servicio[columnas_a_conservar].dropna(axis=1, how='all')
    
    # df_estado_servicio.to_sql('test_dos', ra_fu, if_exists='replace', index_label='key_servicio')
    return df_estado_servicio

def process_data(df_estado_servicio):
    """Procesa los datos para obtener el estado del servicio."""
    # Agrupar los datos y calcular las columnas según el estado_id
    df_agrupado = (df_estado_servicio
                   .groupby(['servicio_id','cliente_id','mensajero_id','sede_id'])
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
    print("TAMANIO DEL DATAFRAME: ",df_agrupado.shape[0])
    print(df_agrupado.head(10))
    df_agrupado.columns.name = None  # Quitar el nombre de las columnas    
    df_agrupado = df_agrupado.dropna(subset=['fecha_recogido','fecha_iniciado','fecha_asignado','fecha_entregado'])
    df_agrupado['fecha_terminado'] = df_agrupado['fecha_terminado'].fillna(df_agrupado['fecha_entregado'])
    
    df_agrupado = df_agrupado.dropna(subset=['hora_recogido','hora_iniciado','hora_asignado','hora_entregado'])
    df_agrupado['hora_terminado'] = df_agrupado['hora_terminado'].fillna(df_agrupado['hora_entregado'])
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
    print("ESTE PROCESO PUEDE TARDAR UN POCO.............................")
    
    # Cargar los datos desde la base de datos
    df_estado_servicio = load_data_from_db(ra_fu)
    
    # Procesar los datos
    df_agrupado = process_data(df_estado_servicio)
    
    # Cargar los datos procesados en la base de datos de destino
    load_to_db(df_agrupado, etl_conn)
    
    print("ETL para 'trans_servicio' completado exitosamente!")
