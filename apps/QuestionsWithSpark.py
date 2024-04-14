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

# Leer datos de las tablas de PostgreSQL y crear DataFrames de Spark
df_ventas = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/retail_db_finally") \
    .option("dbtable", "sales_analysis").option("user", "postgres").option("password", "casa1234").load()

df_geografico = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/retail_db_finally") \
    .option("dbtable", "geographic_analysis").option("user", "postgres").option("password", "casa1234").load()

df_demografico = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/retail_db_finally") \
    .option("dbtable", "demographic_analysis").option("user", "postgres").option("password", "casa1234").load()

df_temporal = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/retail_db_finally") \
    .option("dbtable", "temporal_analysis").option("user", "postgres").option("password", "casa1234").load()

def menu():
    print("Seleccione la pregunta que desea responder:")
    print("1. ¿Qué tienda tiene los mayores ingresos totales?")
    print("2. ¿Cuáles son los ingresos totales generados en una fecha concreta?")
    print("3. ¿Qué producto tiene la mayor cantidad vendida?")
    print("4. ¿Cuáles son las regiones con mejores resultados en función de los ingresos?")
    print("5. ¿Existe alguna correlación entre la ubicación de la tienda y el rendimiento de las ventas?")
    print("6. ¿Cómo varía el rendimiento de las ventas entre los distintos grupos demográficos?")
    print("7. ¿Existen productos específicos preferidos por determinados grupos demográficos?")
    print("8. ¿Cómo varía el rendimiento de las ventas a lo largo del tiempo?")
    print("9. ¿Existen tendencias estacionales en las ventas?")
    print("10. Salir")

    opcion = input("Ingrese el número de la pregunta que desea responder: ")

    return opcion

def responder_pregunta(opcion):
    if opcion == "1":
        df.groupBy("store_id").sum("revenue").orderBy(col("sum(revenue)").desc()).show(1)
    elif opcion == "2":
        fecha_concreta = input("Ingrese la fecha concreta (en formato 'yyyy-MM-dd'): ")
        df.filter(col("date") == fecha_concreta).agg({"revenue": "sum"}).show()
    elif opcion == "3":
        df.groupBy("product_id").sum("quantity_sold").orderBy(col("sum(quantity_sold)").desc()).show(1)
    elif opcion == "4":
        df.groupBy("location").sum("revenue").orderBy(col("sum(revenue)").desc()).show()
    elif opcion == "5":
        print("Esta pregunta puede requerir un análisis más detallado utilizando técnicas de correlación.")
    elif opcion == "6":
        df.groupBy("demographics").sum("revenue").orderBy(col("sum(revenue)").desc()).show()
    elif opcion == "7":
        df.groupBy("demographics", "product_id").sum("quantity_sold").orderBy(col("sum(quantity_sold)").desc()).show()
    elif opcion == "8":
        df.groupBy("date").sum("revenue").orderBy("date").show()
    elif opcion == "9":
        print("")
    elif opcion == "10":
        print("Saliendo del programa...")
        spark.stop()
        exit()
    else:
        print("Opción no válida. Por favor, seleccione un número del 1 al 10.")

# Ejecutar el programa
while True:
    opcion = menu()
    responder_pregunta(opcion)
