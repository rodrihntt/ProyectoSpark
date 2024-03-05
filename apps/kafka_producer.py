from time import sleep
from json import dumps
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

while True:
    
    #[TODO]Tienes que cambiarlo para hacer todos los campos random
    message = {
        "timestamp": int(datetime.now().timestamp() * 1000),
        "store_id": [random nuymb],
        "product_id": "ABC123",
        "quantity_sold": 10,
        "revenue": 500.0
    }
    producer.send('sales_stream', value=message)
    sleep(1)  # Adjust frequency as needed
