import argparse
import json
import random # Library to generate random numbers and make random selections
import threading
import time
from datetime import datetime
from pathlib import Path # Replace the way to handle paths to files with the Path class
from flask import Flask, render_template, jsonify

# Step 2: Read the events from the JSONL file into a Spark DataFrame

BASE = Path(__file__).resolve().parent
PROYECTO_ROOT = BASE.parent
ARCHIVO_EVENTOS = BASE / "eventos.jsonl"
SALIDA_NOTIFICACIONES = BASE / "notificaciones.jsonl"

app = Flask(
    __name__,
    template_folder=str(PROYECTO_ROOT / "Desing"),
    static_folder=str(PROYECTO_ROOT / "Desing"),
    static_url_path="" 
)

# Render the main page of the web application
@app.route('/')
def web():
    return render_template('Index.html')

#Step 3: Transform the data to prepare for consumer analysis

def cargar_eventos(prob_pago, semilla): # Funtion to simulate the events of users adding products to their carts and starting the payment process
    with open(BASE / "carritos.json", encoding="utf-8") as f: 
        carritos = json.load(f)

    carritos.sort(key=lambda c: c["timestamp"]) # Sort the carts by timestamp to simulate the order in which users added products to their carts

    eventos = [] 
    for c in carritos: # Loop through each cart and create an event for each product added to the cart
        eventos.append({
            "tipo_evento": "agregar_carrito",
            "carrito_id": c["carrito_id"],
            "usuario": c["usuario"],
            "email": c["email"],
            "producto": c["producto"],
            "categoria": c["categoria"],
            "cantidad": c["cantidad"],
            "precio_unitario": c["precio_unitario"],
        })

    rnd = random.Random(semilla) # 
    usuarios = sorted({e["usuario"] for e in eventos}) 
    compradores = {u for u in usuarios if rnd.random() < prob_pago} # Create a set of users who will start the payment process based on the probability of payment

    ultimo = {}
    for i, e in enumerate(eventos): # Save the index of the last event for each user to know when to insert the payment initiation event
        ultimo[e["usuario"]] = i

    insertar = {}
    for u in compradores: # For each user who will start the payment process 
        pos = min(ultimo[u] + 3, len(eventos) - 1)
        insertar.setdefault(pos, []).append({
            "tipo_evento": "pago_iniciado", "usuario": u,
            "email": next(e["email"] for e in eventos if e["usuario"] == u),
        })

    cola = []
    for i, e in enumerate(eventos):
        cola.append(e)
        cola.extend(insertar.get(i, []))
    return cola, compradores, usuarios


def enviar(evento): # Create a function to send events to the JSONL file with the current timestamp
    evento["timestamp"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    with open(ARCHIVO_EVENTOS, "a", encoding="utf-8") as f:
        f.write(json.dumps(evento, ensure_ascii=False) + "\n")
    return evento


def generar_eventos_background(intervalo=1.0, prob_pago=0.4, semilla=7): # Given the interval between events, the probability of payment, and a seed for random number generation, generate events in the background
    ARCHIVO_EVENTOS.write_text("", encoding="utf-8")

    cola, compradores, usuarios = cargar_eventos(prob_pago, semilla)
    print(f"{len(cola)} eventos listos | {len(usuarios)} usuarios | "
          f"{len(compradores)} llegarán a pago: {sorted(compradores)}")
    print(f"  => deberían quedar como ABANDONADOS: "
          f"{sorted(set(usuarios) - compradores)}")
    print(f"Escribiendo en {ARCHIVO_EVENTOS.name} (Ctrl+C para terminar)")

    try:
        for e in cola:
            e = enviar(e)
            print(f"[{e['timestamp']}] {e['tipo_evento']:<16} {e['usuario']}"
                  f" {e.get('producto', '')}")
            time.sleep(intervalo)
        print("Todos los eventos enviados. El consumidor los irá procesando solo.")
    except KeyboardInterrupt:
        print("\nProductor detenido.")


@app.route('/api/eventos') # Endoint Flask to return the events in the JSONL file as a JSON array
def api_eventos():
    eventos = []
    if ARCHIVO_EVENTOS.exists():
        with open(ARCHIVO_EVENTOS, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    eventos.append(json.loads(line))
    return jsonify(eventos)


@app.route('/api/notificaciones') # Endpoint Flask to return the notifications only for consumer processing
def api_notificaciones():
    notificaciones = []
    if SALIDA_NOTIFICACIONES.exists():
        with open(SALIDA_NOTIFICACIONES, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    notificaciones.append(json.loads(line))
    return jsonify(notificaciones)


if __name__ == "__main__": # flow in which the producer generates events in the background and the Flask application runs to serve the web interface and API endpoints
    parser = argparse.ArgumentParser()
    parser.add_argument("--intervalo", type=float, default=1.0, help="segundos entre eventos")
    parser.add_argument("--prob-pago", type=float, default=0.4, help="probabilidad de pago")
    parser.add_argument("--semilla", type=int, default=7)
    args = parser.parse_args()

    hilo = threading.Thread(
        target=generar_eventos_background,
        args=(args.intervalo, args.prob_pago, args.semilla),
        daemon=True
    )
    hilo.start()

    app.run(debug=True, port=5000, use_reloader=False)