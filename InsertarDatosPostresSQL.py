import psycopg2
import random
from faker import Faker
import numpy as np

# Configuración de conexión a la base de datos
connection = psycopg2.connect(
    host="localhost",
    port=5432,
    database="retail_db",
    user="postgres",
    password="casa1234"
)

# Crear un cursor para ejecutar comandos SQL
cursor = connection.cursor()

# Crear la tabla "Stores" si no existe
cursor.execute('''
    CREATE TABLE IF NOT EXISTS Stores (
        store_id SERIAL PRIMARY KEY,
        store_name VARCHAR(255),
        location VARCHAR(255),
        demographics VARCHAR(255)
    )
''')

# Confirmar la creación de la tabla
connection.commit()

# Crear datos de manera aleatoria usando Faker
fake = Faker()

# Número de filas a insertar
num_filas = 50

# Insertar datos en la tabla "Stores"
for _ in range(num_filas):
    store_name = fake.company()
    location = fake.address()
    demographics = random.choice(["EEUU", "Alemania", "España"])

    # Insertar datos en la tabla
    cursor.execute("INSERT INTO Stores (store_name, location, demographics) VALUES (%s, %s, %s)",
                   (store_name, location, demographics))

# Datos fake
for _ in range(10):
    store_name = np.nan
    location = " "
    demographics = random.choice([123, 865, 888])

    cursor.execute("INSERT INTO Stores (store_name, location, demographics) VALUES (%s, %s, %s)",
                (store_name, location, demographics))

# Confirmar y cerrar la conexión
connection.commit()
cursor.close()
connection.close()