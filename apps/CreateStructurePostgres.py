import psycopg2
import numpy as np

# Configuración de conexión a la base de datos
connection = psycopg2.connect(
    host="localhost",
    port=5432,
    database="retail_db_finally",
    user="postgres",
    password="casa1234"
)

# Crear un cursor para ejecutar comandos SQL
cursor = connection.cursor()

# Crear la tabla "sales_analysis" si no existe
cursor.execute('''
    CREATE TABLE IF NOT EXISTS sales_analysis (
    sales_id SERIAL PRIMARY KEY,
    date DATE,
    store_id INTEGER,
    product_id VARCHAR(50),
    quantity_sold INTEGER,
    revenue FLOAT
    )'''
    )

# Crear la tabla "geographic_analysis" si no existe
cursor.execute('''
    CREATE TABLE IF NOT EXISTS geographic_analysis (
    sales_id SERIAL PRIMARY KEY,
    date DATE,
    store_id INTEGER,
    product_id VARCHAR(50),
    quantity_sold INTEGER,
    revenue FLOAT
    )'''
    )

# Crear la tabla "demographic_analysis" si no existe
cursor.execute('''
    CREATE TABLE IF NOT EXISTS demographic_analysis (
    sales_id SERIAL PRIMARY KEY,
    date DATE,
    store_id INTEGER,
    product_id VARCHAR(50),
    quantity_sold INTEGER,
    revenue FLOAT
    )'''
    )

# Crear la tabla "temporal_analysis" si no existe
cursor.execute('''
    CREATE TABLE IF NOT EXISTS temporal_analysis (
    sales_id SERIAL PRIMARY KEY,
    date DATE,
    store_id INTEGER,
    product_id VARCHAR(50),
    quantity_sold INTEGER,
    revenue FLOAT
    )'''
    )

# Confirmar la creación de la tabla
connection.commit()

# Confirmar y cerrar la conexión
cursor.close()
connection.close()