
import argparse
import json
import random # Library to generate random numbers and make random selections
import time
from datetime import datetime
from pathlib import Path # Replace the way to handle paths to files with the Path class

# Step 2: Read the events from the JSONL file into a Spark DataFrame

BASE = Path(__file__).resolve().parent
ARCHIVO_EVENTOS = BASE / "eventos.jsonl"

#Step 3: Transform the data to prepare for consumer analysis

def cargar_eventos(prob_pago, semilla): # Funtion to simulate the events of users adding products to their carts and starting the payment process
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--intervalo", type=float, default=1.0,
                    help="segundos entre eventos")
    ap.add_argument("--prob-pago", type=float, default=0.4,
                    help="probabilidad de que un usuario llegue a la pasarela de pago")
    ap.add_argument("--semilla", type=int, default=7)
    args = ap.parse_args()

    # Empezar el archivo desde cero en cada corrida
    ARCHIVO_EVENTOS.write_text("", encoding="utf-8")

    cola, compradores, usuarios = cargar_eventos(args.prob_pago, args.semilla)
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
            time.sleep(args.intervalo)
        print("Todos los eventos enviados. El consumidor los irá procesando solo.")
    except KeyboardInterrupt:
        print("\nProductor detenido.")


if __name__ == "__main__":
    main()