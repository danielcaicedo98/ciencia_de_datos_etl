import pandas as pd
import numpy as np

def create_sede_dim():
    return pd.DataFrame({
        'id_sede': range(1, 11),
        'nombre': [f'Sede {i}' for i in range(1, 11)],
        'direccion': [f'Dirección Sede {i}' for i in range(1, 11)],
        'telefono': [f'555-{str(i).zfill(4)}' for i in range(1, 11)],
        'nombre_contacto': [f'Contacto Sede {i}' for i in range(1, 11)],
        'ciudad_id': np.random.randint(1, 21, 10),
        'cliente_id': np.random.randint(1, 101, 10)
    })