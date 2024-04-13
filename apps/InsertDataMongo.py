from pymongo import MongoClient
import random
from faker import Faker
import numpy as np

# Configuración de conexión a la base de datos MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['retail_db']
collection = db['Stores']

# Crear datos de manera aleatoria usando Faker
fake = Faker()

# Número de documentos a insertar
num_docs = 50

# Insertar datos en la colección "Stores"
for _ in range(num_docs):
    store_name = fake.company()
    location = fake.address()
    demographics = random.choice(["EEUU", "Alemania", "España"])

    # Insertar datos en la colección
    document = {
        "store_name": store_name,
        "location": location,
        "demographics": demographics
    }
    collection.insert_one(document)

# Datos fake
for _ in range(10):
    store_name = np.nan
    location = " "
    demographics = random.choice([123, 865, 888])

    document = {
        "store_name": store_name,
        "location": location,
        "demographics": demographics
    }
    collection.insert_one(document)