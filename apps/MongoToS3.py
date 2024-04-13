from pyspark.sql import SparkSession

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
    
# Configurar la conexión a MongoDB
mongo_uri = "mongodb://localhost:27017/retail_db.Stores"

# Leer datos de MongoDB en un DataFrame de Spark
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", mongo_uri).load()

# Realizar cualquier procesamiento adicional si es necesario

# Escribir el DataFrame en formato CSV
csv_path = "/tmp/stores_data.csv"
df.write.csv(csv_path, header=True, mode="overwrite")

# Configuración de acceso a S3 localstack
aws_access_key_id = 'test'
aws_secret_access_key = 'test'
s3_endpoint = "http://proyectospark-localstack-1:4566"
s3_bucket = "bucket-rodrigo"

# Escribir el archivo CSV en S3 utilizando Spark
df.write \
    .option("header", "true") \
    .csv(f"{s3_endpoint}/{s3_bucket}/sales_data.csv")

# Detener la sesión de Spark
spark.stop()