#Represents the flow of shopping carts, but in function with the producer
import socket
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, unix_timestamp, current_timestamp

# 1. Create SparkSession

spark = SparkSession.builder \
    .appName("E-commerce streaming") \
    .master("local[*]") \
    .getOrCreate()


# 2. Read the file: Example of shopping carts 

carrito_rdd = spark.read.option("multiLine", True).json("carritos.json")

# 3. Convert the text "2026-09-27T18:04:12" to a real timestamp and sort
carritos_df = carrito_rdd.withColumn("timestamp_ts", to_timestamp(col("timestamp")))
order = carritos_df.orderBy(col("timestamp_ts").asc())

# 4. Action every 10 seconds, show the carts that are older than 60 seconds
servidor = socket.socket()
servidor.bind(("localhost", 9999))
servidor.listen(1)
print("Productor esperando al consumidor...")
conexion, _ = servidor.accept()
print("Consumidor conectado")
vistos = set()  # for keeping track of the carts already shown

while True:
    filtrados = order.filter(
        unix_timestamp(current_timestamp()) - unix_timestamp(col("timestamp_ts")) > 60
    )

    if vistos:
        filtrados = filtrados.filter(~col("carrito_id").isin(list(vistos)))

    # Only take the oldest of those that are missing
    siguiente = filtrados.orderBy(col("timestamp_ts").asc()).limit(1).collect()

    if siguiente:
        fila = siguiente[0]
        conexion.sendall((json.dumps(fila.asDict(), default=str, ensure_ascii=False) + "\n").encode("utf-8"))
        vistos.add(fila["carrito_id"])
    else:
        print("No hay más carritos abandonados por mostrar")

    time.sleep(10)  