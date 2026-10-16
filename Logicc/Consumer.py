import os

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from db import guardar_evento, guardar_notificacion

TOPICS = "eventos_carritos,eventos_pagos"
BROKER = "localhost:29092"
GAP_SESION = "20 seconds"
WATERMARK = "10 seconds"
INTERVALO = "5 seconds"
MAX_POR_LOTE = 2000

# ---------- Spark ----------
spark = (SparkSession.builder
         .appName("CarritosAbandonados")
         .master("local[*]")
         .config("spark.driver.host", "127.0.0.1")
         .config("spark.driver.bindAddress", "127.0.0.1")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.driver.memory", "2g")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


class consumer:

    schema = StructType([
        StructField("tipo_evento", StringType()),
        StructField("usuario", StringType()),
        StructField("email", StringType()),
        StructField("producto", StringType()),
        StructField("cantidad", LongType()),
        StructField("precio", DoubleType()),
        StructField("timestamp", StringType()),
    ])

    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", BROKER)
           .option("subscribe", TOPICS)
           .option("startingOffsets", "latest")
           .option("maxOffsetsPerTrigger", MAX_POR_LOTE)
           .load())

    events = (raw
              .select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
              .select("e.*")
              .withColumn("event_time", F.to_timestamp("timestamp"))
              .filter(F.col("event_time").isNotNull())
              .withColumn("valor_linea", F.col("cantidad") * F.col("precio")))

    es_agregar = F.col("tipo_evento") == "agregar_al_carrito"
    es_pago_ok = F.col("tipo_evento") == "pago_exitoso"

    sesiones = (events
                .withWatermark("event_time", WATERMARK)
                .groupBy("usuario", F.session_window("event_time", GAP_SESION).alias("sesion"))
                .agg(
                    F.max("email").alias("email"),
                    F.sum(F.when(es_agregar, F.col("valor_linea")).otherwise(0)).alias("valor_carrito"),
                    F.collect_set(F.when(es_agregar, F.col("producto"))).alias("productos"),
                    F.max(F.when(es_pago_ok, 1).otherwise(0)).alias("pago_exitoso"),
                ))

    abandonados = (sesiones
                   .filter((F.col("pago_exitoso") == 0) & (F.col("valor_carrito") > 0))
                   .select("usuario", "email", "valor_carrito", "productos",
                           F.col("sesion.start").alias("start_sesion")))

    def guardar_lote(batch_df, batch_id):
        filas = [
            (r["tipo_evento"], r["usuario"], r["email"], r["producto"],
             r["cantidad"], r["precio"], r["event_time"])
            for r in batch_df.select("tipo_evento", "usuario", "email", "producto",
                                     "cantidad", "precio", "event_time").collect()
        ]
        guardar_evento(filas)

    def notificar(batch_df, batch_id):
        filas = []
        for r in batch_df.collect():
            productos = ", ".join(sorted(r["productos"]))
            mensaje = (f"Dejaste en tu carrito: {productos} "
                       f"(total ${r['valor_carrito']:,.0f}). ¡Vuelve y termina tu compra!")
            print(f"Notificación: {r['usuario']} ({r['email']}) -> {mensaje}")
            filas.append((r["usuario"], r["email"], productos, float(r["valor_carrito"]), mensaje))
        guardar_notificacion(filas)


if __name__ == "__main__":
    (consumer.events.writeStream
        .foreachBatch(consumer.guardar_lote)
        .option("checkpointLocation", "checkpoints/eventos")
        .trigger(processingTime=INTERVALO)
        .start())

    (consumer.abandonados.writeStream
        .outputMode("append")
        .foreachBatch(consumer.notificar)
        .option("checkpointLocation", "checkpoints/abandonados")
        .trigger(processingTime=INTERVALO)
        .start())

    print("Streaming activo: guardando en MySQL y detectando carritos abandonados...")
    spark.streams.awaitAnyTermination()