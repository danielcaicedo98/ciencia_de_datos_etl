from datetime import timedelta
import pandas as pd
from dimensiones.db_connection import get_database_connections

def extract(source_db):
    try:
        # Extrae la tabla `trans_novedad` de la base de datos
        trans_novedad = pd.read_sql_table('mensajeria_novedadesservicio', source_db)
        return trans_novedad
    
    except Exception as e:
        print(f"Error extracting data: {str(e)}")

def transform(trans_novedad):
    try:
        # Extrae solo la fecha de `fecha_novedad` para la agrupación por día
        trans_novedad['fecha'] = pd.to_datetime(trans_novedad['fecha_novedad']).dt.date

        # Realiza la agregación por fecha y tipo_novedad_id
        novedades_diarias = trans_novedad.groupby(['fecha', 'tipo_novedad_id']).size().reset_index(name='cantidad_novedades')

        return novedades_diarias
    
    except Exception as e:
        print(f"Error transforming data: {str(e)}")

def load(warehouse_db, novedades_diarias):
    try:
        # Carga las novedades diarias en una nueva tabla `novedad_diaria` en el Data Warehouse
        novedades_diarias.to_sql('novedad_diaria', con=warehouse_db, if_exists='replace', index=False)
    
    except Exception as e:
        print(f"Error loading data: {str(e)}")

def run_etl_novedad_agrupada():
    # Conexión a las bases de datos origen y destino
    source_db, warehouse_db = get_database_connections()
    
    # Extracción de datos
    data = extract(source_db)
    
    # Transformación de datos
    transformed = transform(data)
    
    # Carga de datos transformados
    load(warehouse_db, transformed)

if __name__ == "__main__":
    run_etl_novedad_agrupada()
