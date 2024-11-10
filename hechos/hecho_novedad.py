from datetime import timedelta
import pandas as pd
from dimensiones.db_connection import get_database_connections

def extract(source_db):
    try:
        # Extrae la tabla `trans_novedad` de la base de datos
        trans_novedad = pd.read_sql_table('trans_novedad', source_db)
        dim_fecha = pd.read_sql_table('dim_fecha', source_db)
        
        return trans_novedad,dim_fecha
    
    except Exception as e:
        print(f"Error extracting data: {str(e)}")

def transform(trans_novedad,dim_fecha):
    try:
        
        hecho_novedad = pd.merge(trans_novedad, dim_fecha[['date', 'key_dim_fecha']], left_on='fecha', right_on='date', how='left')
        hecho_novedad.drop(columns=['date'], inplace=True)      
        
        hecho_novedad = hecho_novedad[['fecha', 'key_dim_fecha', 'tipo_novedad_id', 'nombre']] 

        # Realiza la agregación por fecha, tipo_novedad_id y nombre
        agrupado = hecho_novedad.groupby(['key_dim_fecha', 'fecha', 'tipo_novedad_id', 'nombre']).size().reset_index(name='novedades_dia')
        
        # Agrega la columna `id` como un identificador incremental
        agrupado.reset_index(drop=True, inplace=True)
        agrupado['id'] = range(1, len(agrupado) + 1)
        
        agrupado = agrupado[['id', 'fecha','key_dim_fecha', 'tipo_novedad_id', 'nombre', 'novedades_dia']]

        return agrupado
    
    except Exception as e:
        print(f"Error transforming data: {str(e)}")

def load(warehouse_db, novedades_diarias):
    try:
        # Carga las novedades diarias en una nueva tabla `novedad_diaria` en el Data Warehouse
        novedades_diarias.to_sql('hecho_novedad', con=warehouse_db, if_exists='replace', index=False)
    
    except Exception as e:
        print(f"Error loading data: {str(e)}")

def run_etl_hecho_novedad():
    # Conexión a las bases de datos origen y destino
    source_db, warehouse_db = get_database_connections()
    
    # Extracción de datos
    trans_novedad,dim_fecha = extract(warehouse_db)
    
    # Transformación de datos
    transformed = transform(trans_novedad,dim_fecha)
    
    # Carga de datos transformados
    load(warehouse_db, transformed)

if __name__ == "__main__":
    run_etl_hecho_novedad()
