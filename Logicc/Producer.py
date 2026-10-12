import json
import random # Library to generate random numbers and make random selections
import threading
import time
from datetime import datetime
from flask import Flask, render_template, jsonify
from kafka import KafkaProducer
from faker import Faker
from collections import deque

# Broker kafka 
productor  = KafkaProducer(
    bootstrap_servers= ['localhost:29092'],
    value_serializer= lambda v: json.dumps(v).encode('utf-8')
)
eventos_recientes = deque(maxlen=50)

class productor:
    def generar_eventos():
        fake = Faker('es_CO')

        USUARIOS = [
            {'usuario': fake.first_name().lower(), 'email': fake.unique.email()}
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
                productor.send('eventos_carritos', value=evento)
                print(f"Enviado: {evento}")
                time.sleep(1)  
        finally:
            productor.flush()
            productor.close()

    # Create conection to the Flask app
    app = Flask(__name__,)

    @app.route('/')
    def web():
        return render_template('Index.html')


    @app.route('/api/eventos')
    def api_eventos():
        return jsonify(list(eventos_recientes)), 200


    if __name__ == '__main__':
        hilo = threading.Thread(target=generar_eventos, daemon=True)
        hilo.start()

        app.run(debug=True, use_reloader=False)

