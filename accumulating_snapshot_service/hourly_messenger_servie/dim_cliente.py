import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def create_cliente_dim():
    n_clients = 100
    start_dates = [datetime(2023, 1, 1) - timedelta(days=np.random.randint(0, 365)) for _ in range(n_clients)]
    
    return pd.DataFrame({
        'nit_cliente': range(1, n_clients + 1),
        'nombre': [f'Cliente {i}' for i in range(1, n_clients + 1)],
        'email': [f'cliente{i}@email.com' for i in range(1, n_clients + 1)],
        'direccion': [f'Dirección {i}' for i in range(1, n_clients + 1)],
        'telefono': [f'555-{str(i).zfill(4)}' for i in range(1, n_clients + 1)],
        'nombre_contacto': [f'Contacto {i}' for i in range(1, n_clients + 1)],
        'ciudad_id': np.random.randint(1, 21, n_clients),
        'tipo_cliente_id': np.random.randint(1, 4, n_clients),
        'activo': np.random.choice([True, False], n_clients),
        'coordinador_id': np.random.randint(1, 11, n_clients),
        'sector': np.random.choice(['Norte', 'Sur', 'Este', 'Oeste', 'Centro'], n_clients),
        'tipo_cliente': np.random.choice(['corporativo', 'individual'], n_clients),
        'clasificacion_cliente': np.random.choice(['A', 'B', 'C'], n_clients),
        'fecha_primera_orden': start_dates,
        'promedio_mensual_envios': np.random.randint(1, 100, n_clients)
    })