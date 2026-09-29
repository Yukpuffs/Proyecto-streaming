import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, unix_timestamp, current_timestamp

spark = SparkSession.builder.appName("Consumidor").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

vistos = set()

while True:
    df = spark.read.option("multiLine", True).json("carritos.json")
    df = df.withColumn("timestamp_ts", to_timestamp(col("timestamp")))

    filtrados = df.filter(
        unix_timestamp(current_timestamp()) - unix_timestamp(col("timestamp_ts")) > 60
    )
    if vistos:
        filtrados = filtrados.filter(~col("carrito_id").isin(list(vistos)))

    siguiente = filtrados.orderBy(col("timestamp_ts").asc()).limit(1).collect()
    if siguiente:
        carrito = siguiente[0].asDict()
        print(f"¡Recordatorio! Correo a {carrito['email']}: Tienes algunos productos que olvidaste en tu carrito: {carrito['producto']}, con un total de {carrito['total']}. ¡No pierdas la oportunidad de completar tu compra!")
        vistos.add(carrito["carrito_id"])
    else:
        print("No hay más carritos abandonados por mostrar")

    time.sleep(10)