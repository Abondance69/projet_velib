"""
Batch Processing avec Apache Spark
Rôle 2 - Data Engineer Batch

Ce script réalise le traitement batch des données Velib avec Spark.
Récupère les données depuis l'API JCDecaux.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, min, max, sum, explode
from pyspark.sql.types import IntegerType, DoubleType
import requests
import json
import os
import tempfile


def create_spark_session(app_name="VelibBatchProcessing"):
    """
    Initialise une SparkSession locale
    
    Args:
        app_name: Nom de l'application Spark
        
    Returns:
        SparkSession: Session Spark configurée
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print(f"✓ SparkSession créée: {app_name}")
    return spark


def fetch_contracts(api_key):
    """
    Récupère la liste de tous les contrats depuis l'API JCDecaux
    
    Args:
        api_key: Clé API JCDecaux
        
    Returns:
        list: Liste des contrats
    """
    url = f"https://api.jcdecaux.com/vls/v3/contracts?apiKey={api_key}"
    print(f"Récupération des contrats depuis l'API...")
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        contracts = response.json()
        print(f"✓ {len(contracts)} contrats récupérés")
        return contracts
    except requests.exceptions.RequestException as e:
        print(f"⚠ Erreur lors de la récupération des contrats: {e}")
        return []


def fetch_stations_from_api(api_key, contract=None):
    """
    Récupère les données des stations depuis l'API JCDecaux
    
    Args:
        api_key: Clé API JCDecaux
        contract: Nom du contrat (optionnel, si None récupère toutes les stations)
        
    Returns:
        list: Liste des stations au format JSON
    """
    if contract:
        url = f"https://api.jcdecaux.com/vls/v3/stations?contract={contract}&apiKey={api_key}"
        print(f"Récupération des stations pour le contrat: {contract}")
    else:
        url = f"https://api.jcdecaux.com/vls/v3/stations?apiKey={api_key}"
        print(f"Récupération de toutes les stations...")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        stations = response.json()
        print(f"✓ {len(stations)} stations récupérées")
        return stations
    except requests.exceptions.RequestException as e:
        print(f"⚠ Erreur lors de la récupération des stations: {e}")
        return []


def load_velib_data_from_api(spark, api_key):
    """
    Charge les données Velib depuis l'API JCDecaux
    
    Args:
        spark: SparkSession active
        api_key: Clé API JCDecaux
        
    Returns:
        DataFrame: Données chargées
    """
    print("=" * 60)
    print("CHARGEMENT DES DONNÉES DEPUIS L'API JCDECAUX")
    print("=" * 60)
    
    # Récupérer toutes les stations
    stations_data = fetch_stations_from_api(api_key)
    
    if not stations_data:
        print("⚠ Aucune donnée récupérée")
        return None
    
    # Sauvegarder dans un fichier temporaire pour éviter les problèmes de timeout
    import tempfile
    temp_file = os.path.join(tempfile.gettempdir(), "velib_temp.json")
    with open(temp_file, 'w', encoding='utf-8') as f:
        json.dump(stations_data, f)
    
    # Créer un DataFrame Spark depuis le fichier JSON
    df = spark.read.json(temp_file)
    
    print(f"✓ DataFrame créé avec {df.count()} stations")
    return df


def clean_data(df):
    """
    Nettoie et transforme les données depuis l'API JCDecaux
    
    Args:
        df: DataFrame brut
        
    Returns:
        DataFrame: Données nettoyées
    """
    print("Nettoyage des données...")
    
    # Extraire les champs imbriqués (nested JSON)
    df_clean = df.select(
        col("number").alias("station_number"),
        col("contractName").alias("contract_name"),
        col("name").alias("station_name"),
        col("address"),
        col("position.latitude").alias("latitude"),
        col("position.longitude").alias("longitude"),
        col("banking"),
        col("bonus"),
        col("status"),
        col("connected"),
        col("totalStands.capacity").alias("capacity"),
        col("totalStands.availabilities.bikes").alias("bikes_available"),
        col("totalStands.availabilities.stands").alias("stands_available"),
        col("totalStands.availabilities.mechanicalBikes").alias("mechanical_bikes"),
        col("totalStands.availabilities.electricalBikes").alias("electrical_bikes"),
        col("lastUpdate").alias("last_update")
    )
    
    # Supprimer les lignes avec des valeurs nulles critiques
    df_clean = df_clean.dropna(subset=["station_name", "capacity"])
    
    # Filtrer les stations ouvertes uniquement
    df_clean = df_clean.filter(col("status") == "OPEN")
    
    print(f"✓ Données nettoyées: {df_clean.count()} stations ouvertes")
    return df_clean


def compute_station_stats(df):
    """
    Calcule les statistiques par station et par contrat
    
    Args:
        df: DataFrame nettoyé
        
    Returns:
        DataFrame: Statistiques agrégées par station
    """
    print("Calcul des statistiques par station...")
    
    # Agrégation par contrat et station
    stats = df.groupBy("contract_name", "station_name") \
        .agg(
            max("capacity").alias("capacity_total"),
            avg("bikes_available").alias("bikes_avg"),
            min("bikes_available").alias("bikes_min"),
            max("bikes_available").alias("bikes_max"),
            avg("mechanical_bikes").alias("mechanical_avg"),
            avg("electrical_bikes").alias("electrical_avg"),
            count("*").alias("nb_observations")
        ) \
        .orderBy(col("capacity_total").desc())
    
    print(f"✓ Statistiques calculées pour {stats.count()} stations")
    return stats


def compute_global_stats(df):
    """
    Calcule les statistiques globales
    
    Args:
        df: DataFrame nettoyé
        
    Returns:
        dict: Statistiques globales
    """
    print("Calcul des statistiques globales...")
    
    total_capacity = df.agg(sum("capacity")).collect()[0][0]
    avg_capacity = df.agg(avg("capacity")).collect()[0][0]
    nb_stations = df.select("station_name").distinct().count()
    nb_contracts = df.select("contract_name").distinct().count()
    
    stats = {
        "total_capacity": total_capacity,
        "avg_capacity": round(avg_capacity, 2) if avg_capacity else 0,
        "nb_stations": nb_stations,
        "nb_contracts": nb_contracts,
        "total_records": df.count()
    }
    
    print("✓ Statistiques globales:")
    for key, value in stats.items():
        print(f"  - {key}: {value}")
    
    return stats


def save_to_mongodb(df, collection_name="velib_batch_stats"):
    """
    Sauvegarde les résultats dans MongoDB
    
    Args:
        df: DataFrame à sauvegarder
        collection_name: Nom de la collection MongoDB
    """
    print(f"Sauvegarde dans MongoDB (collection: {collection_name})...")
    
    # Configuration MongoDB (à adapter selon votre environnement)
    mongo_uri = "mongodb://localhost:27017/velib_db"
    
    try:
        df.write \
            .format("mongodb") \
            .mode("append") \
            .option("uri", mongo_uri) \
            .option("collection", collection_name) \
            .save()
        
        print(f"✓ Données sauvegardées dans MongoDB: {collection_name}")
    except Exception as e:
        print(f"⚠ Erreur lors de la sauvegarde MongoDB: {e}")
        print("  (Vérifiez que le connecteur MongoDB Spark est installé)")


def main():
    """
    Fonction principale - Pipeline batch complet avec API JCDecaux
    """
    print("=" * 60)
    print("DÉMARRAGE DU TRAITEMENT BATCH VELIB - API JCDECAUX")
    print("=" * 60)
    
    # Configuration API
    API_KEY = "0a2ee2814ab033c2a34c4ae92df2c9b0cf1a5471"
    
    # Étape 1: Initialisation Spark
    spark = create_spark_session()
    
    # Étape 2: Récupération des contrats
    contracts = fetch_contracts(API_KEY)
    if contracts:
        print("\nContrats disponibles:")
        for contract in contracts[:10]:  # Afficher les 10 premiers
            print(f"  - {contract.get('name')} ({contract.get('commercial_name')})")
        if len(contracts) > 10:
            print(f"  ... et {len(contracts) - 10} autres contrats")
    
    # Étape 3: Chargement des données depuis l'API
    df = load_velib_data_from_api(spark, API_KEY)
    
    if df is None:
        print("⚠ Impossible de charger les données")
        spark.stop()
        return
    
    # Étape 4: Exploration rapide
    print("\n" + "=" * 60)
    print("SCHÉMA DES DONNÉES")
    print("=" * 60)
    df.printSchema()
    
    print("\n" + "=" * 60)
    print("APERÇU DES DONNÉES BRUTES (5 premières lignes)")
    print("=" * 60)
    df.show(5, truncate=False, vertical=True)
    
    # Étape 5: Nettoyage
    df_clean = clean_data(df)
    
    print("\n" + "=" * 60)
    print("APERÇU DES DONNÉES NETTOYÉES")
    print("=" * 60)
    df_clean.show(10, truncate=False)
    
    # Étape 6: Calculs
    station_stats = compute_station_stats(df_clean)
    global_stats = compute_global_stats(df_clean)
    
    # Étape 7: Affichage des résultats
    print("\n" + "=" * 60)
    print("RÉSULTATS - TOP 20 STATIONS PAR CAPACITÉ")
    print("=" * 60)
    station_stats.show(20, truncate=False)
    
    # Statistiques par contrat
    print("\n" + "=" * 60)
    print("STATISTIQUES PAR CONTRAT")
    print("=" * 60)
    contract_stats = df_clean.groupBy("contract_name") \
        .agg(
            count("*").alias("nb_stations"),
            sum("capacity").alias("total_capacity"),
            avg("capacity").alias("avg_capacity"),
            sum("bikes_available").alias("total_bikes")
        ) \
        .orderBy(col("nb_stations").desc())
    contract_stats.show(20, truncate=False)
    
    # Étape 8: Sauvegarde (optionnelle)
    # Décommenter quand MongoDB est configuré
    # save_to_mongodb(station_stats, "velib_batch_stats")
    
    print("\n" + "=" * 60)
    print("TRAITEMENT BATCH TERMINÉ")
    print("=" * 60)
    
    # Arrêt de Spark
    spark.stop()


if __name__ == "__main__":
    main()
