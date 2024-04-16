from neo4j import GraphDatabase
import csv

def connect_to_neo4j(uri, user, password):
    """Conecta a una base de datos Neo4j."""
    driver = GraphDatabase.driver(uri, auth=(user, password))
    return driver

def run_query(driver, query):
    """Ejecuta una consulta en la base de datos Neo4j y devuelve los resultados."""
    with driver.session() as session:
        result = session.run(query)
        return result.data()

def save_to_csv(records, csv_filename):
    """Guarda los resultados de una consulta Neo4j en un archivo CSV."""
    with open(csv_filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(records[0].keys())
        for record in records:
            writer.writerow(record.values())

# Configuración de conexión a Neo4j
neo4j_uri = "bolt://localhost:7687"
neo4j_user = "neo4j"
neo4j_password = "casa1234"

# Consulta a ejecutar en Neo4j
query = "MATCH (n:Person) RETURN n.name AS name, n.age AS age, n.city AS city, n.interest AS interest"

# Nombre del archivo CSV de salida
csv_filename = "neo4j_data.csv"

# Conectar a Neo4j y ejecutar la consulta
driver = connect_to_neo4j(neo4j_uri, neo4j_user, neo4j_password)
result = run_query(driver, query)

# Guardar los resultados en un archivo CSV
save_to_csv(result, csv_filename)

# Cerrar la conexión a Neo4j
driver.close()

print("Los datos se han guardado correctamente en el archivo CSV:", csv_filename)
