from pyspark.sql import SparkSession

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

# Cerrar la sesión de Spark
spark.stop()