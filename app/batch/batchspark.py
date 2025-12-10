from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    avg,
    min as spark_min,
    max as spark_max,
    sum as spark_sum
)

# ============================================================
#  ⚙️ Initialisation de la SparkSession + config MongoDB
# ============================================================
spark = (
    SparkSession.builder
    .appName("velib-batch")
    .master("local[*]")  # OK dans ton contexte docker
    .config(
        "spark.jars.packages",
        "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
    )
    # URI par défaut (écriture)
    .config(
        "spark.mongodb.write.connection.uri",
        "mongodb://admin:pwd@mongodb-ipssi:27017/"
        "velib.velib_batch_capacity?authSource=admin"
    )
    # URI par défaut (lecture si besoin)
    .config(
        "spark.mongodb.read.connection.uri",
        "mongodb://admin:pwd@mongodb-ipssi:27017/"
        "velib.velib_batch_capacity?authSource=admin"
    )
    .getOrCreate()
)

print("🚀 Spark session started")

# ============================================================
#  📥 Lecture du CSV Velib depuis HDFS
# ============================================================
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ";")
    .csv("hdfs://namenode:9000/users/ipssi/input/velib.csv")
)

print("📄 Données brutes :")
df.show(5)

# ============================================================
#  🧹 Nettoyage / typage des colonnes utiles
# ============================================================
df_clean = (
    df
    .withColumn("capacity", col("capacity").cast("int"))
    .withColumn("stationcode", col("stationcode").cast("int"))
    .withColumn("numdocksavailable", col("numdocksavailable").cast("int"))
    .withColumn("numbikesavailable", col("numbikesavailable").cast("int"))
    .withColumn("mechanical", col("mechanical").cast("int"))
    .withColumn("ebike", col("ebike").cast("int"))
)

# ============================================================
#  🧮 Calculs batch
# ============================================================

# 🔹 Capacité totale par station (par nom de station)
capacity_by_station = (
    df_clean
    .groupBy("name")
    .agg(spark_sum("capacity").alias("total_capacity"))
)

# 🔹 Statistiques globales sur la capacité
stats = df_clean.select(
    avg("capacity").alias("moyenne"),
    spark_min("capacity").alias("min"),
    spark_max("capacity").alias("max"),
)

print("🏁 Capacité totale par station :")
capacity_by_station.show(20, truncate=False)

print("📊 Statistiques globales :")
stats.show()

# ============================================================
#  💾 Écriture dans MongoDB (ROLE 2)
# ============================================================
print("💾 Insertion dans MongoDB...")

# 1️⃣ Table agrégée par station
capacity_by_station.write \
    .format("mongodb") \
    .mode("append") \
    .option("connection.uri", "mongodb://admin:pwd@mongodb-ipssi:27017") \
    .option("database", "velib") \
    .option("collection", "velib_batch_capacity") \
    .option("authSource", "admin") \
    .save()

# 2️⃣ Table des stats globales
stats.write \
    .format("mongodb") \
    .mode("append") \
    .option("connection.uri", "mongodb://admin:pwd@mongodb-ipssi:27017") \
    .option("database", "velib") \
    .option("collection", "velib_batch_stats") \
    .option("authSource", "admin") \
    .save()

print("✅ Données batch écrites dans Mongo !")

spark.stop()
