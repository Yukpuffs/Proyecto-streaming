import os
import json
import time
from datetime import datetime

# Windows: necesario para que Spark arranque
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"

from kafka import KafkaConsumer
from db import guardar_evento, limpiar_eventos
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# Configuración
TOPIC = "eventos_carritos"
BROKER = "localhost:29092"
GAP_SESION = "20 seconds"        
BUFFER_CIERRE_SEGUNDOS = 10      
INTERVALO_REVISION = 10          
SALIDA = "notificaciones.jsonl"
INICIO = datetime.now()          

vistos = set()                   

# Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BROKER],
    auto_offset_reset='latest',
    group_id='grupo-carritos',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Spark 
spark = (SparkSession.builder
         .appName("CarritosAbandonados")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.jars.packages",f"org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("tipo_evento", StringType()),
    StructField("usuario", StringType()),
    StructField("email", StringType()),
    StructField("producto", StringType()),
    StructField("cantidad", LongType()),
    StructField("precio", DoubleType()),
    StructField("timestamp", StringType()),
])


class Consumer:
    def __init__(self):
        self.consumer = consumer
        self.ultima_revision = time.time()

    def obtener_eventos(self): # Funtion to get events from Kafka and save them to MySQL
        for mensaje in self.consumer:
            dato = mensaje.value
            try:
                fecha = datetime.fromisoformat(dato['timestamp'])
                guardar_evento(dato['tipo_evento'], dato['usuario'], dato['email'],
                               dato['producto'], dato['cantidad'], dato['precio'], fecha)
            except Exception as e:
                print(f"Error guardando {dato}: {e}")

            # Check if it's time to calculate abandoned carts
            if time.time() - self.ultima_revision >= INTERVALO_REVISION: 
                self.ultima_revision = time.time()
                try:
                    self.calcular_y_notificar()
                except Exception as e:
                    print(f"Error calculando carritos abandonados: {e}")

    def calcular_y_notificar(self):
        raw = (spark.read
               .format("kafka")
               .option("kafka.bootstrap.servers", BROKER)
               .option("subscribe", TOPIC)
               .option("startingOffsets", "earliest")
               .option("endingOffsets", "latest")
               .load())

        events = (raw
                  .select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
                  .select("e.*")
                  .withColumn("event_time", F.to_timestamp("timestamp"))
                  .filter(F.col("event_time").isNotNull())
                  .filter(F.col("event_time") >= F.lit(INICIO))   # ignora datos anteriores
                  .withColumn("valor_linea", F.col("cantidad") * F.col("precio")))

        es_agregar = F.col("tipo_evento") == "agregar_al_carrito"

        sesion_user = (events
                       .groupBy("usuario", F.session_window("event_time", GAP_SESION).alias("sesion"))
                       .agg(
                           F.max("email").alias("email"),
                           F.sum(F.when(es_agregar, F.col("valor_linea")).otherwise(0)).alias("valor_carrito"),
                           F.collect_set(F.when(es_agregar, F.col("producto"))).alias("productos"),
                           F.max(F.when(F.col("tipo_evento") == "iniciar_pago", 1).otherwise(0)).alias("inicio_pago"),
                       ))

        cerradas = sesion_user.filter(
            (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.col("sesion.end")))
            > BUFFER_CIERRE_SEGUNDOS
        )

        abandonados = cerradas.filter((F.col("inicio_pago") == 0) & (F.col("valor_carrito") > 0))

        resultado = abandonados.select(
            "usuario", "email", "valor_carrito", "productos",
            F.col("sesion.start").alias("start_sesion")
        )

        filas = resultado.collect()
        nuevas = [r for r in filas if (r["usuario"], r["start_sesion"]) not in vistos]

        if not nuevas:
            return

        with open(SALIDA, "a", encoding="utf-8") as out:
            for r in nuevas:
                vistos.add((r["usuario"], r["start_sesion"]))
                productos = ", ".join(sorted(r["productos"]))
                mensaje = (f"Dejaste en tu carrito: {productos} "
                           f"(total ${r['valor_carrito']:,.0f}). ¡Vuelve y termina tu compra!")

                out.write(json.dumps({
                    "usuario": r["usuario"],
                    "email": r["email"],
                    "valor_carrito": r["valor_carrito"],
                    "productos": sorted(r["productos"]),
                    "mensaje": mensaje,
                }, ensure_ascii=False) + "\n")

                print(f"Notificación: {r['usuario']} ({r['email']}) -> {mensaje}")


if __name__ == "__main__":
    limpiar_eventos() 
    print("Escuchando eventos y guardando en MySQL...")
    Consumer().obtener_eventos()