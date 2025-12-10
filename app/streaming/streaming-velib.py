from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col

# =====================================================
# ÉTAPE 2 – Spark Streaming SANS Mongo, SANS HDFS
# Source : dossier local dans le conteneur (/spark-apps/stream-input)
# =====================================================

# 1) Création de la SparkSession
# Pour l'instant on reste en local dans le conteneur.
# -----------------------------------------------------
# TODO MONGO PLUS TARD :
# Quand Mongo sera configuré, tu pourras rajouter ici :
#   .config("spark.mongodb.write.connection.uri",
#           "mongodb://user:pwd@mongo:27017/velib.velib_streaming_stats")
# et éventuellement changer le master en "spark://spark-master:7077"
# -----------------------------------------------------
spark = SparkSession.builder \
    .appName("velib_streaming_etape2") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2) Lecture STATIQUE pour déduire le schéma à partir de velib.csv
#    (une seule fois, pas du streaming ici)
static_df = spark.read \
    .option("header", "true") \
    .option("sep", ";") \
    .csv("/spark-apps/stream-input/velib.csv")

# On caste numbikesavailable en entier pour pouvoir faire des sommes
static_df = static_df.withColumn(
    "numbikesavailable",
    col("numbikesavailable").cast("int")
)

print("====== SCHÉMA DÉDUIT À PARTIR DE velib.csv ======")
static_df.printSchema()

schema = static_df.schema  # schéma qu'on va réutiliser pour le streaming

# 3) Lecture EN STREAMING du dossier surveillé
#    Ici Spark va surveiller /spark-apps/stream-input dans le conteneur.
streamDf = spark.readStream \
    .schema(schema) \
    .option("header", "true") \
    .option("sep", ";") \
    .csv("/spark-apps/stream-input/")
    #quand abondance fait le HDFS je change cela #
    #.csv("hdfs://namenode:9000/users/ipssi/input/velib2")#

print("Le DataFrame est-il en streaming ?", streamDf.isStreaming)

# 4) TRAITEMENT STREAMING – Étape 2
#    Exemple demandé : groupBy("name").agg(sum("numbikesavailable"))
aggDf = streamDf.groupBy("name") \
    .agg(sum("numbikesavailable").alias("total_bikes"))

# 5) SORTIE – Affichage en console
#    C'est ce qui valide ton Étape 2 : voir les résultats dans la console.
query_console = aggDf.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# -----------------------------------------------------
# TODO MONGO PLUS TARD :
# Quand Mongo sera prêt, tu pourras ajouter par exemple :
#
# from pyspark.sql.functions import current_timestamp
#
# def write_row(batch_df, batch_id):
#     # (optionnel) ajouter un timestamp
#     # batch_df = batch_df.withColumn("batch_time", current_timestamp())
#     (batch_df.write
#         .format("mongodb")
#         .mode("append")
#         .save())
#
# query_mongo = aggDf.writeStream \
#     .outputMode("update") \
#     .foreachBatch(write_row) \
#     .start()
#
# et utiliser query_mongo.awaitTermination() à la place
# -----------------------------------------------------

# 6) On laisse tourner le streaming
query_console.awaitTermination()
