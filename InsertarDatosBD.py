import psycopg2
import random
from faker import Faker

# Configuración de conexión a la base de datos
connection = psycopg2.connect(
    host="localhost",
    port=5432,
    database="retail_db"
)

# Crear un cursor para ejecutar comandos SQL
cursor = connection.cursor()

# Crear datos de manera aleatoria usando Faker
fake = Faker()

# Número de filas a insertar
num_filas = 10

# Insertar datos en la tabla "Stores"
for _ in range(num_filas):
    store_name = fake.company()
    location = fake.address()
    demographics = {"age_group": random.choice(["18-25", "26-35", "36-45"]),
                   "income_range": random.choice(["Low", "Medium", "High"])}

    # Insertar datos en la tabla
    cursor.execute("INSERT INTO Stores (store_name, location, demographics) VALUES (%s, %s, %s)",
                   (store_name, location, demographics))

# Confirmar y cerrar la conexión
connection.commit()
cursor.close()
connection.close()