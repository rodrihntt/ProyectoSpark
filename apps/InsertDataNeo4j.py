from neo4j import GraphDatabase
from pyspark.sql import SparkSession

# Configuración de la conexión a Neo4j
neo4j_uri = "bolt://localhost:7687"
neo4j_user = "neo4j"
neo4j_password = "casa1234"

# Configuración de la conexión a LocalStack S3
aws_access_key_id = 'test'
aws_secret_access_key = 'test'

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("SPARK S3") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://proyectospark-localstack-1:4566") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.1.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.1.jar") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Función para conectar a Neo4j y ejecutar una consulta para obtener los datos
def get_data_from_neo4j():
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    with driver.session() as session:
        result = session.run("MATCH (n:Person) RETURN n.name AS name, n.age AS age, n.city AS city, n.interest AS interest")
        data = [record for record in result]
    driver.close()
    return data

# Función principal
def main():
    # Obtener datos de Neo4j
    data = get_data_from_neo4j()

    # Crear un DataFrame Spark a partir de los datos
    df = spark.createDataFrame(data, ["name", "age", "city", "interest"])

    # Escribir el DataFrame en S3
    df.write.mode("overwrite").csv("s3a://my-localstack-bucket/data")

if __name__ == "__main__":
    main()