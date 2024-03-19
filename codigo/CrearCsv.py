import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# Configuración de parámetros
num_rows = 10000  # Número de filas de datos
null_percentage = 0.01  # Porcentaje de valores nulos

# Función para generar fechas aleatorias en un rango
def random_dates(start_date, end_date, n=10):
    date_range = [start_date + timedelta(days=random.randint(0, (end_date - start_date).days)) for _ in range(n)]
    return date_range

# Definir el rango de fechas
start_date = datetime(2022, 1, 1)
end_date = datetime(2023, 12, 31)

# Generar datos aleatorios
data = {
    'Date': random_dates(start_date, end_date, num_rows),
    'Store ID': [random.randint(1, 10) for _ in range(num_rows)],
    'Product ID': ['P' + str(random.randint(1, 5)) for _ in range(num_rows)],
    'Quantity Sold': [random.randint(1, 50) for _ in range(num_rows)],
    'Revenue': [round(random.uniform(10, 1000), 2) for _ in range(num_rows)],
}

# Introducir valores nulos aleatorios
for column in data:
    null_indices = random.sample(range(num_rows), int(null_percentage * num_rows))
    for idx in null_indices:
        data[column][idx] = np.nan

# Crear DataFrame
df = pd.DataFrame(data)

# Introducir algunos errores de formato
df['Date'].iloc[0] = '2022-01-XX'
df['Date'].iloc[10] = '2022-XX-10'
df['Date'].iloc[20] = 'XXXX-01-XX'
df['Quantity Sold'].iloc[1] = 'abc'
df['Quantity Sold'].iloc[100] = 'fa'
df['Quantity Sold'].iloc[1000] = 'kjg'
df['Revenue'].iloc[2] = np.nan  # Cambiado para ser consistente con la columna numérica
df['Revenue'].iloc[200] = np.nan
df['Revenue'].iloc[205] = np.nan

# Cambiar formato de la columna 'Date'
df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.strftime('%Y-%m-%d')

# Guardar DataFrame en un archivo CSV
df.to_csv('data/sales_data.csv', index=False, sep=',')

print("Archivo 'sales_data.csv' creado con éxito.")