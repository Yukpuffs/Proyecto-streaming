import argparse
import json
import random
import threading
import time
from datetime import datetime
from pathlib import Path
from flask import Flask, render_template, jsonify

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

# Renderiza la página principal
@app.route('/')
def web():
    return render_template('Index.html')


def cargar_eventos(prob_pago, semilla):
    with open(BASE / "carritos.json", encoding="utf-8") as f: 
        carritos = json.load(f)

    carritos.sort(key=lambda c: c["timestamp"])

    eventos = []
    for c in carritos:
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

    rnd = random.Random(semilla) 
    usuarios = sorted({e["usuario"] for e in eventos})
    compradores = {u for u in usuarios if rnd.random() < prob_pago}

    ultimo = {}
    for i, e in enumerate(eventos):
        ultimo[e["usuario"]] = i

    insertar = {}
    for u in compradores:
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


def enviar(evento):
    evento["timestamp"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    with open(ARCHIVO_EVENTOS, "a", encoding="utf-8") as f:
        f.write(json.dumps(evento, ensure_ascii=False) + "\n")
    return evento


def generar_eventos_background(intervalo=1.0, prob_pago=0.4, semilla=7):
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


# Endpoint para cargar eventos en vivo del Productor
@app.route('/api/eventos')
def api_eventos():
    eventos = []
    if ARCHIVO_EVENTOS.exists():
        with open(ARCHIVO_EVENTOS, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    eventos.append(json.loads(line))
    return jsonify(eventos)


# Endpoint para cargar las notificaciones generadas por Spark (Consumidor)
@app.route('/api/notificaciones')
def api_notificaciones():
    notificaciones = []
    if SALIDA_NOTIFICACIONES.exists():
        with open(SALIDA_NOTIFICACIONES, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    notificaciones.append(json.loads(line))
    return jsonify(notificaciones)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--intervalo", type=float, default=1.0, help="segundos entre eventos")
    parser.add_argument("--prob-pago", type=float, default=0.4, help="probabilidad de pago")
    parser.add_argument("--semilla", type=int, default=7)
    args = parser.parse_args()

    # Inicia el generador de eventos en segundo plano
    hilo = threading.Thread(
        target=generar_eventos_background,
        args=(args.intervalo, args.prob_pago, args.semilla),
        daemon=True
    )
    hilo.start()

    app.run(debug=True, port=5000, use_reloader=False)