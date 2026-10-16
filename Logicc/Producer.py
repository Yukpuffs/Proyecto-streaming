import json
import random #Import library for random number generation
import threading # Import library for multithreading
import time
from datetime import datetime
from flask import Flask, render_template, jsonify # Import flask for web server and API
from kafka import KafkaProducer, KafkaConsumer # Import Kafka producer and consumer
from faker import Faker # Import library for generating fake data
from collections import deque
from db import obtener_notificacion # Import function to get notifications from the database

eventos_recientes = deque(maxlen=50) # Create a deque to store the last 50 events
pagos_recientes = deque(maxlen=50) # Create a deque to store the last 50 payment events

class productor: # Class to generate events and send them to Kafka
    def __init__(self):
        # Broker kafka 
        self.productor = KafkaProducer(
            bootstrap_servers=['localhost:29092'], # Kafka server address
            value_serializer=lambda v: json.dumps(v).encode('utf-8') # Serialize the event data to JSON format and encode it to bytes
        )

    def generar_eventos(self): # Function to generate events and send them to Kafka
        fake = Faker('es_CO') # Create a Faker object to generate fake data in Spanish 

        USUARIOS = [
            {'usuario': fake.unique.first_name().lower(), 'email': fake.unique.email()}
            for _ in range(20)
        ]
        PRODUCTOS = [f"producto_{i}" for i in range(1, 16)]

        TIPO_EVENTOS = ['agregar_al_carrito', 'iniciar_pago'] # List of event types to generate
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
                time.sleep(1) # Wait for 1 second before generating the next event
        finally:
            self.productor.flush()
            self.productor.close()


    def escuchar_pagos(): # Function to listen for payment events from Kafka and store them in a deque
        c = KafkaConsumer(
            'eventos_pagos',
            bootstrap_servers=['localhost:29092'],
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        for mensaje in c:
            pagos_recientes.appendleft(mensaje.value)


# Create conection to the Flask app
app = Flask(__name__, template_folder='../Desing', static_folder='../Desing', static_url_path='') # Create a Flask app with the template and static folders set to the Desing folder

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
    threading.Thread(target=productor.escuchar_pagos, daemon=True).start()

    app.run(debug=True, use_reloader=False)