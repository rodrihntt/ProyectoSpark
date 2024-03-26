from pyspark.sql import SparkSession
import boto3
import os

# Iniciar una sesión de Spark
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .getOrCreate()
    
# Ruta del archivo CSV
ruta_csv = "../spark-data/sales_data.csv"

# Leer el archivo CSV como un DataFrame de Spark
datos_df = spark.read.csv(ruta_csv, header=True, sep=",", inferSchema=True)

# Mostrar el esquema y las primeras filas del DataFrame
datos_df.printSchema()
datos_df.show()

# Convertir el DataFrame a formato Parquet
ruta_temporal_parquet = "../spark-data/sales_data.parquet"
datos_df.write.parquet(ruta_temporal_parquet, mode="overwrite")

# Cerrar la sesión de Spark
spark.stop()

# Nombre del archivo en S3
nombre_archivo_s3_parquet = "sales_data.parquet"

# Crear un cliente S3
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:4566',  # URL de LocalStack
    aws_access_key_id='test',  # clave de acceso ficticia (predeterminada de LocalStack)
    aws_secret_access_key='test',  # clave secreta ficticia (predeterminada de LocalStack)
)

# Ruta del archivo Parquet
ruta_parquet = "../spark-data/sales_data.parquet"

# Cargar el archivo Parquet al bucket de S3
s3.upload_file(ruta_parquet, "bucket-rodrigo", nombre_archivo_s3_parquet)

print(f"Archivo '{nombre_archivo_s3_parquet}' cargado exitosamente en el bucket 'bucket-rodrigo'.")

# Eliminar el archivo temporal Parquet
os.remove(ruta_parquet)