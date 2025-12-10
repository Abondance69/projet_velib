from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max

# 1) SparkSession en mode local
spark = SparkSession.builder \
    .appName("velib-batch-local") \
    .master("local[*]") \
    .getOrCreate()

print("🚀 Spark local session started")

# 2) Lire le CSV local AVEC LE BON DELIMITER
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("delimiter", ";") \
    .csv("hdfs://namenode:9000/users/ipssi/input/velib.csv")

print("📄 Données brutes :")
df.show(5)

# 3) Nettoyage / cast des colonnes
df_clean = df.withColumn("capacity", col("capacity").cast("int"))

# 4) Calculs batch
# Capacité totale par station
capacity_by_station = df_clean.groupBy("name").sum("capacity")

print("🏁 Capacité totale par station :")
capacity_by_station.show()

# Statistiques globales
stats = df_clean.select(
    avg("capacity").alias("moyenne"),
    min("capacity").alias("min"),
    max("capacity").alias("max")
)

print("📊 Statistiques :")
stats.show()

spark.stop()

print("👋 Spark session stopped")