from pyspark.sql import SparkSession

aws_access_key_id = 'test'
aws_secret_access_key = 'test'

spark = SparkSession.builder \
    .appName("SPARK S3") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://proyectospark-localstack-1:4566") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages","org.apache.spark:spark-hadoop-cloud_2.13:3.5.1,software.amazon.awssdk:s3:2.25.11") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

try:
    # Configuración de conexión a PostgreSQL
    postgres_url = "jdbc:postgresql://localhost:5432/retail_db"
    postgres_properties = {"user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}

    # Cargar datos desde PostgreSQL
    df = spark.read.jdbc(url=postgres_url, table="Stores", properties=postgres_properties)

    # Escribir el DataFrame en S3
    df.write \
        .option('fs.s3a.committer.name', 'directory') \
        .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
        .option("fs.s3a.fast.upload.buffer", "bytebuffer") \
        .mode('overwrite') \
        .csv(path='s3a://nombre_de_tu_bucket/postgres.csv')

    spark.stop()
    
except Exception as e:
    print("Error al leer o escribir en S3:")
    print(e)