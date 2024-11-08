from datetime import timedelta
import pandas as pd
from sqlalchemy import text
from dimensiones.db_connection import get_database_connections

def extract(source_db):
    try:
        # Extraer datos de las tablas de transacciones y dimensiones necesarias
        trans_servicio = pd.read_sql_table('trans_servicio', source_db)
        dim_fecha = pd.read_sql_table('dim_fecha', source_db)
        dim_hora = pd.read_sql_table('dim_hora', source_db)
        
        # Filtrar solo los estados relevantes
        trans_servicio = trans_servicio[trans_servicio['estado'].isin(['mensajero asignado', 'tiene novedad', 'recogido por mensajero', 'entregado en destino'])]
        
        # Unir las fechas y horas a `trans_servicio` para obtener claves de fecha y hora
        trans_servicio = pd.merge(trans_servicio, dim_fecha[['date', 'key_dim_fecha']], left_on='fecha_iniciado', right_on='date', how='left')
        trans_servicio.drop(columns=['date'], inplace=True)
        trans_servicio.rename(columns={'key_dim_fecha': 'key_fecha_iniciado'}, inplace=True)

        trans_servicio = pd.merge(trans_servicio, dim_hora[['time', 'key_dim_hora']], left_on='hora_iniciado', right_on='time', how='left')
        trans_servicio.drop(columns=['time'], inplace=True)
        trans_servicio.rename(columns={'key_dim_hora': 'key_hora_iniciado'}, inplace=True)

        return trans_servicio

    except Exception as e:
        print(f"Error extracting data: {str(e)}")

def transform(trans_servicio):
    try:
        # Convertir las horas a formato datetime, ignorando minutos y segundos
        trans_servicio['hora_iniciado'] = pd.to_datetime(trans_servicio['hora_iniciado'].dt.strftime('%H:00:00'), format='%H:%M:%S')
        trans_servicio['hora_terminado'] = pd.to_datetime(trans_servicio['hora_terminado'].dt.strftime('%H:00:00'), format='%H:%M:%S')
        
        # Crear un DataFrame para almacenar las horas ocupadas
        horas_ocupadas = []
        
        for servicio_id, group in trans_servicio.groupby('servicio_id'):
            # Identificar el primer y último estado en el rango ocupado para cada servicio
            inicio = group[group['estado'] == 'mensajero asignado'].iloc[0]['hora_iniciado']
            fin = group[group['estado'] == 'entregado en destino'].iloc[-1]['hora_terminado']
            
            # Dividir el tiempo en intervalos de una hora
            hora_actual = inicio
            while hora_actual <= fin:
                horas_ocupadas.append({
                    'servicio_id': servicio_id,
                    'fecha': group['fecha_iniciado'].iloc[0],  # Fecha del inicio
                    'hora': hora_actual.hour
                })
                hora_actual += timedelta(hours=1)
        
        # Convertir a DataFrame
        df_horas_ocupadas = pd.DataFrame(horas_ocupadas)
        
        # Contar servicios activos por hora
        horas_agrupadas = df_horas_ocupadas.groupby(['fecha', 'hora']).size().reset_index(name='servicios_activos')

        return horas_agrupadas

    except Exception as e:
        print(f"Error transforming data: {str(e)}")

def load(warehouse_db, horas_agrupadas):
    try:
        # Cargar datos a la tabla de hechos en el almacén de datos
        horas_agrupadas.to_sql('hecho_servicio_mensajeria_hora', con=warehouse_db, if_exists='replace', index=False)
    except Exception as e:
        print(f"Error loading data: {str(e)}")

def run_etl_servicio_mensajeria_hora():
    # Obtener conexiones a bases de datos
    source_db, warehouse_db = get_database_connections()
  
    # Extraer los datos
    data = extract(source_db)

    # Transformar los datos
    transformed = transform(data)

    # Cargar los datos al warehouse
    load(warehouse_db, transformed)

if __name__ == "__main__":
    run_etl_servicio_mensajeria_hora()
