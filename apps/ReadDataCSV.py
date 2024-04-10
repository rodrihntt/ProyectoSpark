from pyspark.sql import SparkSession

aws_access_key_id = 'test'
aws_secret_access_key = 'test'

spark = SparkSession.builder \
    .appName("SPARK S3") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://spark-localstack-1:4566") \
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
    
# Ruta del archivo CSV
ruta_csv = "sales_data.csv"

# Leer el archivo CSV como un DataFrame de Spark
datos_df = spark.read.csv(ruta_csv, header=True, sep=",", inferSchema=True)

# Especificar la ruta del bucket de S3 en LocalStack
s3_bucket = "s3a://bucket-rodrigo"

# Escribir el DataFrame en el bucket de S3
datos_df.write.csv(s3_bucket, mode="overwrite")

# Cerrar la sesión de Spark
spark.stop()