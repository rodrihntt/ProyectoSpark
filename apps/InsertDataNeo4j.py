from neo4j import GraphDatabase
from faker import Faker
import random

# Configuración de la conexión a Neo4j
neo4j_uri = "bolt://localhost:7687"
neo4j_user = "neo4j"
neo4j_password = "casa1234"

# Función para conectar a Neo4j y ejecutar una consulta para insertar datos
def insert_data_to_neo4j(data):
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    with driver.session() as session:
        for name, age, city, interest in data:
            session.run(
                "CREATE (p:Person {name: $name, age: $age, city: $city, interest: $interest})",
                name=name, age=age, city=city, interest=interest
            )
    driver.close()

# Función para generar datos aleatorios
def generate_random_data(num_records):
    fake = Faker()
    data = []
    for _ in range(num_records):
        name = fake.name()
        age = random.randint(18, 80)
        city = fake.city()
        interest = fake.word()
        data.append((name, age, city, interest))
    return data

# Función principal
def main():
    # Generar datos aleatorios
    data = generate_random_data(100)

    # Insertar datos en Neo4j
    insert_data_to_neo4j(data)
    print("Datos insertados en Neo4j correctamente.")

if __name__ == "__main__":
    main()