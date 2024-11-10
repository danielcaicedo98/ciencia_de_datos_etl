from datetime import timedelta
import pandas as pd
from dimensiones.db_connection import get_database_connections

def extract(source_db):
    try:
        # Extrae la tabla `trans_novedad` de la base de datos
        mens_nov = pd.read_sql_table('mensajeria_novedadesservicio', source_db)
        tipo_novedad = pd.read_sql_table('mensajeria_tiponovedad', source_db)  
        
        return mens_nov,tipo_novedad
    
    except Exception as e:
        print(f"Error extracting data: {str(e)}")

def transform(mens_nov,tipo_novedad ):
    try:
        tipo_novedad.rename(columns={'id': 'id_novedad'}, inplace=True)
        trans_novedad = pd.merge(mens_nov, tipo_novedad, left_on='tipo_novedad_id', right_on='id_novedad', how='inner')        
        trans_novedad['fecha'] = pd.to_datetime(trans_novedad['fecha_novedad']).dt.date        
        trans_novedad.reset_index(drop=True, inplace=True)
        trans_novedad['id'] = range(1, len(trans_novedad) + 1)
        trans_novedad['fecha'] = pd.to_datetime(trans_novedad['fecha'].astype(str) + ' 00:00:00')
        return trans_novedad
    
    except Exception as e:
        print(f"Error transforming data: {str(e)}")

def load(warehouse_db, novedades_diarias):
    try:
        # Carga las novedades diarias en una nueva tabla `novedad_diaria` en el Data Warehouse
        novedades_diarias.to_sql('trans_novedad', con=warehouse_db, if_exists='replace', index=False)
    
    except Exception as e:
        print(f"Error loading data: {str(e)}")

def run_etl_trans_novedad():
    # Conexión a las bases de datos origen y destino
    source_db, warehouse_db = get_database_connections()
    
    # Extracción de datos
    mens_nov,tipo_novedad = extract(source_db)
    
    # Transformación de datos
    transformed = transform(mens_nov,tipo_novedad )
    
    # Carga de datos transformados
    load(warehouse_db, transformed)

if __name__ == "__main__":
   run_etl_trans_novedad()
