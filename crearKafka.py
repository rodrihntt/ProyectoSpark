import random
from time import sleep
from json import dumps
from kafka import KafkaProducer
from datetime import datetime

# Configurar el productor de Kafka
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

while True:
    # Generar datos aleatorios para cada campo
    timestamp = int(datetime.now().timestamp() * 1000)  # UNIX timestamp en milisegundos
    store_id = random.choice([1001, None])  # Algunos valores de store_id pueden ser nulos
    product_id = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890', k=6))  # ID de producto aleatorio
    quantity_sold = random.randint(1, 50)  # Cantidad vendida aleatoria
    
    # Introducir valores nulos, vacíos o errores de formato de manera aleatoria
    if random.random() < 0.1:
        store_id = None
    if random.random() < 0.1:
        product_id = ''  # Introduce un valor vacío para product_id
    if random.random() < 0.1:
        quantity_sold = 'error'  # Introduce un error de formato para quantity_sold
    
    # Si el valor de quantity_sold es un error de formato, debemos manejarlo para evitar errores de serialización JSON
    try:
        quantity_sold = int(quantity_sold)
    except ValueError:
        quantity_sold = None
    
    revenue = round(random.uniform(10.0, 1000.0), 2)  # Ingresos aleatorios
    
    # Crear el mensaje
    message = {
        "timestamp": timestamp,
        "store_id": store_id,
        "product_id": product_id,
        "quantity_sold": quantity_sold,
        "revenue": revenue
    }
    
    # Enviar el mensaje al tema 'sales_stream' de Kafka
    producer.send('sales_stream', value=message)
    
    # Esperar antes de enviar el siguiente mensaje (ajusta según sea necesario)
    sleep(1)