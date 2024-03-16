from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean

def limpieza_datos(spark):
    # Leer datos desde diferentes fuentes
    df_csv = spark.read.option("header", "true").csv("/ProyectoSpark/data/sales_data.csv")
    df_postgres = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/retail_db") \
        .option("dbtable", "Stores") \
        .option("user", "postgres") \
        .option("password", "casa1234") \
        .load()
    df_kafka = spark.read.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sales_stream") \
        .load()

    # Realizar tareas de limpieza en cada DataFrame
    dfs = [df_csv, df_postgres, df_kafka]
    dfs_limpios = []

    for df in dfs:
        # Eliminar filas duplicadas
        df_sin_duplicados = df.dropDuplicates()

        # Imputar valores perdidos
        columnas_numericas = [c for c, t in df_sin_duplicados.dtypes if t in ('int', 'double')]
        medias = df_sin_duplicados.select(*(mean(col(c)).alias(c) for c in columnas_numericas)).collect()[0].asDict()
        df_imputado = df_sin_duplicados.fillna(medias)

        # Eliminar filas con valores nulos o vacíos
        df_limpiado = df_imputado.dropna()

        dfs_limpios.append(df_limpiado)

    # Mostrar los DataFrames limpios
    for i, df_limpio in enumerate(dfs_limpios):
        print(f"Datos limpios del origen {i + 1}:")
        df_limpio.show()

    # Guardar los DataFrames limpios en nuevos archivos CSV
    for i, df_limpio in enumerate(dfs_limpios):
        df_limpio.write.option("header", "true").csv(f"/ProyectoSpark/data/limpio_origen_{i + 1}.csv", mode="overwrite")

if __name__ == "__main__":
    # Iniciar sesión de Spark
    spark = SparkSession.builder \
        .appName("Limpieza de datos") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    limpieza_datos(spark)

    # Detener la sesión de Spark
    spark.stop()