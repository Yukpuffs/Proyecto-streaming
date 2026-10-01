import json
import time
from datetime import datetime
from pathlib import Path # Replace the way to handle paths to files with the Path class

from pyspark.sql import SparkSession # Library to create a SparkSession and work with Spark DataFrames
from pyspark.sql import functions as F # Library to use functions in Spark DataFrames (columns, aggregations, etc.)

BASE = Path(__file__).resolve().parent # Different way to get the path of the current file and its parent directory
ARCHIVO_EVENTOS = BASE / "eventos.jsonl"

GAP_SESION = "1 minute" # Time of inactivity to consider a session closed
BUFFER_CIERRE_SEGUNDOS = 60 # Time extra to wait before considering a session closed
SALIDA = BASE / "notificaciones.jsonl"

# Step 1: Create a SparkSession

spark = (SparkSession.builder
         .appName("CarritosAbandonados")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "4") # divides the information into segments to process it 
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

SALIDA.write_text("", encoding="utf-8")

vistos = set() # A colection to keep track of already notified abandoned carts


def calcular_y_notificar():
    if not ARCHIVO_EVENTOS.exists() or ARCHIVO_EVENTOS.stat().st_size == 0:
        return

    # Step 2: Read the events from the JSONL file into a Spark DataFrame

    events = (spark.read.json(str(ARCHIVO_EVENTOS))
               
               #Step 3: Transform the data to prepare for session analysis

               .withColumn("event_time", F.to_timestamp("timestamp")) # Create a new column with the timestamp in datetime format
               .filter(F.col("event_time").isNotNull()) # Filter out rows with null timestamps
               .withColumn("valor_linea", F.col("cantidad") * F.col("precio_unitario"))) # Create a new column with the total for all products in the cart

    es_agregar = F.col("tipo_evento") == "agregar_carrito"

    sesion_user = (events
                .groupBy("usuario", F.session_window("event_time", GAP_SESION).alias("sesion")) # Group events by user and session window
                .agg( # Aggregate the data to get the necessary information for each session
                    F.max("email").alias("email"), # Get the email of the user
                    F.sum(F.when(es_agregar, F.col("valor_linea")).otherwise(0)).alias("valor_carrito"),
                    F.collect_set(F.when(es_agregar, F.col("producto"))).alias("productos"),
                    F.max(F.when(F.col("tipo_evento") == "pago_iniciado", 1).otherwise(0)).alias("inicio_pago"),
                ))

    cerradas = sesion_user.filter(
        (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.col("sesion.end"))) # Filter sessions that have ended more than BUFFER_CIERRE_SEGUNDOS seconds ago
        > BUFFER_CIERRE_SEGUNDOS
    )

    abandonados = cerradas.filter((F.col("inicio_pago") == 0) & (F.col("valor_carrito") > 0)) # Filter sessions that have not started payment and have a cart value greater than 0

    resultado = abandonados.select( # Only select the necessary columns for the final result
        "usuario", "email", "valor_carrito", "productos",
        F.col("sesion.start").alias("start_sesion")
    )

    # Step 4: Collect the results and notify users about their abandoned carts

    filas = resultado.collect() # Collect the results into a list of rows
    nuevas = [r for r in filas if (r["usuario"], r["start_sesion"]) not in vistos] # Go through each row to identify which users have not been reviewed
    if not nuevas:
        return

    print(f"\n===== {len(nuevas)} carrito(s) abandonado(s) nuevo(s) =====")
    with open(SALIDA, "a", encoding="utf-8") as out:
        for r in nuevas: # It goes through each row and marks it as viewed
            vistos.add((r["usuario"], r["start_sesion"]))
            productos = ", ".join(sorted(r["productos"])) # Sort the products in the cart 

            mensaje = (f"Dejaste en tu carrito: {productos} " # Create the message to notify the user about their abandoned cart
                       f"(total ${r['valor_carrito']:,}). ¡Vuelve y termina tu compra!")
            print(f"[CORREO a {r['email']}] {mensaje}")

            out.write(json.dumps({
                "usuario": r["usuario"], "email": r["email"],
                "valor_carrito": r["valor_carrito"], "productos": sorted(r["productos"]),
                "mensaje": mensaje,
            }, ensure_ascii=False) + "\n")


print(f"Consumidor revisando {ARCHIVO_EVENTOS.name} cada 5s | sesión={GAP_SESION}")
while True:
    calcular_y_notificar()
    time.sleep(5)