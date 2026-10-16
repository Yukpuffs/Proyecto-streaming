import json
import random
import threading
import time
from datetime import datetime
from flask import Flask, render_template, jsonify
from kafka import KafkaProducer, KafkaConsumer
from faker import Faker
from collections import deque
from db import obtener_notificacion

eventos_recientes = deque(maxlen=50)
pagos_recientes = deque(maxlen=50)

class productor:
    def __init__(self):
        # Broker kafka 
        self.productor = KafkaProducer(
            bootstrap_servers=['localhost:29092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def generar_eventos(self):
        fake = Faker('es_CO')

        USUARIOS = [
            {'usuario': fake.unique.first_name().lower(), 'email': fake.unique.email()}
            for _ in range(20)
        ]
        PRODUCTOS = [f"producto_{i}" for i in range(1, 16)]

        TIPO_EVENTOS = ['agregar_al_carrito', 'iniciar_pago']
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
                self.productor.send('eventos_carritos', value=evento)
                eventos_recientes.appendleft(evento)
                print(f"Enviado: {evento}")
                time.sleep(1)
        finally:
            self.productor.flush()
            self.productor.close()


def escuchar_pagos():
    c = KafkaConsumer(
        'eventos_pagos',
        bootstrap_servers=['localhost:29092'],
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    for mensaje in c:
        pagos_recientes.appendleft(mensaje.value)


# Create conection to the Flask app
app = Flask(__name__, template_folder='../Desing', static_folder='../Desing', static_url_path='')

@app.route('/')
def web():
    return render_template('Index.html')

@app.route('/api/eventos')
def api_eventos():
    return jsonify(list(eventos_recientes)), 200


@app.route('/api/eventos2')
def api_eventos2():
    return jsonify(list(pagos_recientes)), 200

@app.route('/api/notificaciones')
def api_notificaciones():
    return jsonify(obtener_notificacion()), 200

if __name__ == '__main__':
    genn = productor()
    hilo = threading.Thread(target=genn.generar_eventos, daemon=True)
    hilo.start()
    threading.Thread(target=escuchar_pagos, daemon=True).start()

    app.run(debug=True, use_reloader=False)