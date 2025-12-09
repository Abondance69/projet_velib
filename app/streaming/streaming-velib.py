from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col

# 1) Création de la SparkSession
spark = SparkSession.builder \
    .appName("velib_streaming") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2) Lecture STATIQUE pour déduire le schéma à partir de velib.csv
static_df = spark.read \
    .option("header", "true") \
    .option("sep", ";") \
    .csv("/spark-apps/stream-input/velib.csv")

print("====== SCHÉMA DÉDUIT À PARTIR DE velib.csv ======")
static_df.printSchema()

# On caste numbikesavailable en entier (si besoin)
static_df = static_df.withColumn("numbikesavailable", col("numbikesavailable").cast("int"))

schema = static_df.schema  # on récupère le schéma corrigé

# 3) Lecture EN STREAMING du DOSSIER avec le schéma trouvé
streamDf = spark.readStream \
    .schema(schema) \
    .option("header", "true") \
    .option("sep", ";") \
    .csv("/spark-apps/stream-input/")
    #quand abondance fait le HDFS je change cela 
    #.csv("hdfs://namenode:9000/users/ipssi/input/velib2")


print("Le DataFrame est-il en streaming ?", streamDf.isStreaming)

# 4) Exemple : stations où numbikesavailable == 0
dfclean = streamDf.where(col("numbikesavailable") == 0)

query_clean = dfclean.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# 5) Exemple : somme des vélos disponibles par station
aggDf = streamDf.groupBy("name").agg(sum("numbikesavailable").alias("total_bikes"))

query_agg = aggDf.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# 6) On attend que le streaming tourne
query_agg.awaitTermination()
