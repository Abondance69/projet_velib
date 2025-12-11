from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import (
    col, avg, window, to_timestamp
)

# ============================================================
# 🚀 1. SparkSession avec connecteur MongoDB
# ============================================================
spark = (
    SparkSession.builder
    .appName("velib-streaming-api")
    .master("local[*]")
    .config("spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0")
    .config("spark.mongodb.write.connection.uri",
            "mongodb://admin:pwd@mongodb-ipssi:27017/?authSource=admin")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("🚀 Spark session started (Streaming API)")

# ============================================================
# 📌 2. Schéma NDJSON généré par le collecteur
# ============================================================
schema = StructType([
    StructField("number", IntegerType(), True),
    StructField("contract_name", StringType(), True),
    StructField("name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("bike_stands", IntegerType(), True),
    StructField("available_bike_stands", IntegerType(), True),
    StructField("available_bikes", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("last_update", LongType(), True),
    StructField("collection_timestamp", StringType(), True),
    StructField("position", StructType([
        StructField("lat", DoubleType(), True),
        StructField("lng", DoubleType(), True),
    ]), True)
])

# ============================================================
# 📂 3. Streaming depuis HDFS
# ============================================================
HDFS_PATH = "hdfs://namenode:9000/users/ipssi/input/velib_api/contract=Lyon/"

df_stream = (
    spark.readStream
        .schema(schema)
        .json(HDFS_PATH)
)

# ============================================================
# 🧼 4. Nettoyage + horodatage exploitable
# ============================================================
df_clean = df_stream.select(
    col("number").alias("stationcode"),
    col("name"),
    col("bike_stands").alias("capacity"),
    col("available_bikes"),
    col("available_bike_stands"),
    col("position.lat").alias("lat"),
    col("position.lng").alias("lon"),
    to_timestamp(col("collection_timestamp"), "yyyyMMdd_HHmmss").alias("ts")
)

# ============================================================
# 📊 5. Statistiques en streaming avec fenêtre glissante
# ============================================================
agg_stream = (
    df_clean
        .withWatermark("ts", "10 minutes")
        .groupBy(
            window(col("ts"), "10 minutes", "5 minutes"),
            col("stationcode"),
            col("name")
        )
        .agg(
            avg("available_bikes").alias("avg_available_bikes"),
            avg("available_bike_stands").alias("avg_available_stands")
        )
)

# ============================================================
# 💾 6. Écriture vers MongoDB (append)
# ============================================================
query = (
    agg_stream.writeStream
        .format("mongodb")
        .option("checkpointLocation", "/tmp/velib_stream_checkpoint")
        .option("database", "velib")
        .option("collection", "velib_stream_api")
        .outputMode("append")          # ✔️ OBLIGATOIRE avec MongoDB
        .start()
)

print("🔥 Streaming en cours… Données envoyées en continu vers MongoDB !")

query.awaitTermination()
