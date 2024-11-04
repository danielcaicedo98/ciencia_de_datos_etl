import pandas as pd

def create_tipo_servicio_dim():
    return pd.DataFrame({
        'id_tipo_servicio': range(1, 5),
        'nombre': ['Express', 'Regular', 'Económico', 'Premium'],
        'descripcion': [
            'Entrega en 2 horas',
            'Entrega en 24 horas',
            'Entrega en 48-72 horas',
            'Entrega personalizada'
        ]
    })