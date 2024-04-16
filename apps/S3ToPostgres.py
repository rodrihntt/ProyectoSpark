from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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
    # Leer los datos limpios del Data Lake
    df = spark.read.option("header", "true").csv("s3://bucket-rodrigo-clean/")

    # Transformar y cargar los datos en la tabla de análisis de ventas en PostgreSQL
    df_sales = df.select(
        col("Date").alias("date"),
        col("Store ID").alias("store_id"),
        col("Product ID").alias("product_id"),
        col("Quantity Sold").alias("quantity_sold"),
        col("Revenue").alias("revenue")
    )

    # Escribir los datos en la tabla de análisis de ventas en PostgreSQL
    df_sales.write.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/retail_db_finally") \
        .option("dbtable", "sales_analysis") \
        .option("user", "postgres") \
        .option("password", "casa1234") \
        .mode("overwrite") \
        .save()

    # Transformar y cargar los datos en la segunda tabla (por ejemplo, análisis geográfico)
    df_geo = df.select(
        col("Store ID").alias("store_id"),
        col("Location").alias("location"),
        col("Demographics").alias("demographics")
    )

    # Escribir los datos en la segunda tabla en PostgreSQL
    df_geo.write.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/retail_db_finally") \
        .option("dbtable", "geo_analysis") \
        .option("user", "postgres") \
        .option("password", "casa1234") \
        .mode("overwrite") \
        .save()

    spark.stop()

except Exception as e:
    print("Error al procesar o escribir en PostgreSQL:")
    print(e)