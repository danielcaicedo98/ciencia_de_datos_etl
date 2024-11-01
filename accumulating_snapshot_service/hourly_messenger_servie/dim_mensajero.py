import pandas as pd
import numpy as np

def create_mensajero_dim():
    return pd.DataFrame({
        'id_mensajero': range(1, 51),
        'nombre': [f'Mensajero {i}' for i in range(1, 51)],
        'Fecha_entrada': pd.date_range(start='2022-01-01', periods=50),
        'Fecha_salida': None,
        'salario': np.random.uniform(1000, 2000, 50),
        'telefono': [f'555-{str(i).zfill(4)}' for i in range(1, 51)],
        'ciudad_operacion_id': np.random.randint(1, 21, 50),
        'activo': np.random.choice([True, False], 50, p=[0.9, 0.1])
    })