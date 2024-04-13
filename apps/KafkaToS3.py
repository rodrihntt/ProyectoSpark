from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from json import loads

# Configuración de acceso a S3 localstack
aws_access_key_id = 'test'
aws_secret_access_key = 'test'

# Iniciar una sesión Spark
spark = SparkSession.builder \
    .appName("KafkaToS3") \
    .getOrCreate()

# Set up Kafka consumer
consumer = KafkaConsumer(
    'sales_stream',                       # Topic to subscribe to
    bootstrap_servers=['localhost:9092'], # Kafka broker(s)
    auto_offset_reset='earliest',        # Start from earliest message
    enable_auto_commit=True,             # Commit offsets automatically
    value_deserializer=lambda x: x.decode('utf-8') 
)

try:
    for message in consumer:
        # Parsear el mensaje JSON
        data = loads(message.value)

        # Convertir el mensaje a DataFrame Spark
        df = spark.createDataFrame([data])

        # Escribir el DataFrame directamente en el bucket de S3 simulado por LocalStack
        df.write \
            .option('fs.s3a.committer.name', 'directory') \
            .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
            .option("fs.s3a.fast.upload.buffer", "bytebuffer") \
            .mode('append') \
            .parquet("s3a://nombre-del-bucket/data.parquet")

except KeyboardInterrupt:
    print("Interrupción del usuario. Cerrando el consumidor de Kafka.")

finally:
    # Cerrar el consumidor de Kafka
    consumer.close()

    # Detener la sesión Spark
    spark.stop()