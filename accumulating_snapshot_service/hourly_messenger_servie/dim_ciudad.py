import pandas as pd
import numpy as np

def create_ciudad_dim():
    n_cities = 20
    return pd.DataFrame({
        'id_ciudad_destino': range(1, n_cities + 1),
        'direccion': [f'Ciudad Dirección {i}' for i in range(1, n_cities + 1)],
        'cliente_id': np.random.randint(1, 101, n_cities),
        'ciudad_id': range(1, n_cities + 1),
        'departamento_id': np.random.randint(1, 11, n_cities),
        'zona_geografica': np.random.choice(['Norte', 'Sur', 'Este', 'Oeste', 'Centro'], n_cities),
        'tipo_zona': np.random.choice(['urbana', 'rural'], n_cities),
        'distancia_centro_distribucion': np.random.uniform(1, 100, n_cities),
        'tiempo_promedio_entrega': np.random.uniform(0.5, 48, n_cities)
    })