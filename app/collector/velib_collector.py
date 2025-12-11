import requests
import json
import time
import datetime
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# ==========================
# 🔧 CONFIG SPARK + HDFS
# ==========================
spark = (
    SparkSession.builder
    .appName("velib-collector")
    .master("local[*]")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Charger .env
load_dotenv()

API_KEY = os.getenv("API_KEY", "ce84ca5171706ca19a98d34be1ab52854e47cc6f")
CONTRACT = "Lyon"

# Dossier HDFS (URI complet)
# HDFS_BASE_PATH = "hdfs://namenode:9000/user/jovyan/velib_api"

HDFS_BASE_PATH = "hdfs://namenode:9000/users/ipssi/input/velib_api"


def fetch_velib_data():
    """Récupère les données de l'API JCDecaux."""
    url = "https://api.jcdecaux.com/vls/v1/stations"
    params = {
        "apiKey": API_KEY,
        "contract": CONTRACT
    }

    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def save_to_hdfs_with_spark(data):
    """Envoie les données dans HDFS via Spark (JSON)."""
    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    temp_file = f"/tmp/velib_temp_{ts}.json"

    # 1) Écrire NDJSON local
    with open(temp_file, 'w') as f:
        for record in data:
            record['collection_timestamp'] = ts
            f.write(json.dumps(record) + '\n')

    # 2) Charger avec Spark
    df = spark.read.json(f"file://{temp_file}")

    # 3) Écrire dans HDFS
    out_path = f"{HDFS_BASE_PATH}/contract={CONTRACT}"
    df.write.mode("append").json(out_path)

    # 4) Actions Spark → nécessitent le fichier encore présent
    row_count = df.count()
    print(f"✅ Batch {ts} écrit dans HDFS : {out_path}")
    print(f"   → {row_count} lignes écrites")

    print("📊 Aperçu des données :")
    df.select("number", "name", "available_bikes", "bike_stands").show(5, truncate=False)

    # 5) ❗ Supprimer le fichier UNIQUEMENT APRÈS toutes les actions
    os.remove(temp_file)



def main():
    # 15 minutes
    interval_seconds = 900

    print("=" * 70)
    print("🚀 COLLECTEUR VÉLIB → HDFS (via Spark)")
    print("=" * 70)
    print(f"📍 Ville : {CONTRACT}")
    print(f"⏰ Intervalle : {interval_seconds}s ({interval_seconds//60} min)")
    print(f"📂 Destination HDFS : {HDFS_BASE_PATH}")
    print("=" * 70)

    iteration = 0

    while True:
        try:
            iteration += 1
            print(f"\n{'='*70}")
            print(f"🔄 ITÉRATION #{iteration}")
            print(f"{'='*70}")
            
            print("🛰 Récupération des données API…")
            data = fetch_velib_data()
            print(f"📥 {len(data)} stations reçues")

            save_to_hdfs_with_spark(data)

        except Exception as e:
            print(f"❌ Erreur : {e}")
            import traceback
            traceback.print_exc()

        print(f"\n⏲ Pause {interval_seconds} sec…")
        print(f"{'='*70}")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()