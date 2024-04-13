from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Configuración de acceso a S3 localstack
aws_access_key_id = 'test'
aws_secret_access_key = 'test'

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("SPARK S3") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://proyectospark-localstack-1:4566") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .master("local[*]") \
    .getOrCreate()

try:
    # Configurar la ruta de los archivos CSV en tu bucket S3
    s3_input_path = "s3://bucket-rodrigo/"

    # Leer los archivos CSV desde el bucket S3
    df = spark.read.option("header", "true").csv(s3_input_path)

    # Realizar la limpieza de datos (eliminar valores nulos)
    df_cleaned = df.dropna()

    # Escribir el DataFrame de PostgreSQL en S3
    df_cleaned.write \
        .option('fs.s3a.committer.name', 'directory') \
        .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
        .option("fs.s3a.fast.upload.buffer", "bytebuffer") \
        .mode('overwrite') \
        .csv(path='s3a://bucket-rodrigo-clean/')

    spark.stop()

except Exception as e:
    print("Error al procesar o escribir en S3:")
    print(e)
