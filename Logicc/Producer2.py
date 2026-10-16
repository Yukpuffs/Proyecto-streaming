import json
import random 
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker
import time


class productor2:
    def __init__(self):
        # Broker kafka 
        self.productor = KafkaProducer(
            bootstrap_servers=['localhost:29092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def generar_pagos(self):
        fake = Faker('es_CO')

        USUARIOS = [
            {'usuario': fake.unique.first_name().lower(), 'email': fake.unique.email()}
            for _ in range(20)
        ]
        PRODUCTOS = [f"producto_{i}" for i in range(1, 16)]

        TIPO_EVENTOS = ['pago_exitoso', 'pago_fallido', 'pago_rechazado']
        print("Generando eventos")

        try:
            while True:
                user = random.choice(USUARIOS)
                evento = {
                    'tipo_evento': random.choice(TIPO_EVENTOS),
                    'usuario': user['usuario'],
                    'email': user['email'],
                    'producto': random.choice(PRODUCTOS),
                    'cantidad': random.randint(1, 10),
                    'precio': round(random.uniform(3000, 50000), 2),
                    'timestamp': datetime.now().isoformat()
                }
                self.productor.send('eventos_pagos', value=evento)
                print(f"Enviado: {evento}")
                time.sleep(1)  
        finally:
            self.productor.flush()
            self.productor.close()


if __name__ == '__main__':
    gen = productor2()
    gen.generar_pagos()