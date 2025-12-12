from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, min as spark_min, max as spark_max
)

# ============================================================
# 🚀 1. SparkSession avec connecteur MongoDB
# ============================================================
spark = (
    SparkSession.builder
    .appName("velib-batch-api")
    .master("local[*]")
    .config("spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0")
    .config("spark.mongodb.write.connection.uri",
            "mongodb://admin:pwd@mongodb-ipssi:27017/?authSource=admin")
    .config("spark.mongodb.read.connection.uri",
            "mongodb://admin:pwd@mongodb-ipssi:27017/?authSource=admin")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("🚀 Spark session started (batch API)")


# ============================================================
# 📂 2. Lecture des données API stockées dans HDFS
# ============================================================
HDFS_PATH = "hdfs://namenode:9000/users/ipssi/input/velib_api/contract=Lyon/*.json"

df_raw = spark.read.json(HDFS_PATH)

print("📄 Données brutes API :")
df_raw.show(5, truncate=False)


# ============================================================
# 🧼 3. Nettoyage / sélection
# ============================================================
df_clean = df_raw.select(
    col("number").alias("stationcode"),
    col("name"),
    col("contract_name"),
    col("bike_stands").alias("capacity"),
    col("available_bikes"),
    col("available_bike_stands"),
    col("status"),
    col("position.lat").alias("lat"),
    col("position.lng").alias("lon"),
    col("last_update"),
    col("collection_timestamp")
).withColumn("capacity", col("capacity").cast("int"))

print("🧹 Données nettoyées :")
df_clean.show(5, truncate=False)


# ============================================================
# 📊 4. Calculs batch
# ============================================================

# ---- Statistiques par station ----
capacity_by_station = (
    df_clean.groupBy("stationcode", "name")
    .agg(
        spark_max("capacity").alias("max_capacity"),
        avg("available_bikes").alias("avg_available_bikes"),
        avg("available_bike_stands").alias("avg_available_stands")
    )
)

print("🏁 Statistiques par station :")
capacity_by_station.show(20, truncate=False)

# ---- Statistiques globales ----
stats_globales = df_clean.select(
    avg("capacity").alias("moyenne_capacity"),
    spark_min("capacity").alias("min_capacity"),
    spark_max("capacity").alias("max_capacity")
)

print("📊 Statistiques globales :")
stats_globales.show()


# ============================================================
# 💾 5. Écriture dans MongoDB
# ============================================================
print("💾 Insertion des résultats batch dans Mongo...")

capacity_by_station.write \
    .format("mongodb") \
    .mode("overwrite") \
    .option("database", "velib") \
    .option("collection", "velib_batch_api_by_station") \
    .option("authSource", "admin") \
    .save()

stats_globales.write \
    .format("mongodb") \
    .mode("overwrite") \
    .option("database", "velib") \
    .option("collection", "velib_batch_api_stats") \
    .option("authSource", "admin") \
    .save()

print("✅ Données batch API écrites dans Mongo !")


# ============================================================
# 📴 6. Stop Spark
# ============================================================
spark.stop()
print("👋 Spark session stopped")
