#!/usr/bin/env python3
"""
TP 2a (Jour 2) - Ingestion : couche BRONZE
==========================================

Objectif pedagogique (C28) : ingerer 5 sources heterogenes SANS rien corriger.
La couche bronze est une copie fidele du reel, enrichie de metadonnees
techniques. C'est ce qui rend un pipeline rejouable et auditable.

Regles de la couche bronze :
  1. schema EXPLICITE pour les sources critiques (jamais inferSchema en prod)
  2. aucune correction metier : les donnees sales restent sales
  3. on ajoute la tracabilite : fichier source, date d'ingestion, lot
  4. sortie en Parquet partitionne par date d'ingestion

A executer :  python 02_tp2_bronze_ingestion.py
"""

import os
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType)

from mobicity import chemins, ecrire_parquet, spark_session

C = chemins()
LOT = datetime.now().strftime("%Y%m%d_%H%M%S")
DATE_INGESTION = datetime.now().strftime("%Y-%m-%d")


def tracer(df, source: str):
    """Metadonnees d'ingestion communes a toutes les sources (tracabilite)."""
    return (df
            .withColumn("_source", F.lit(source))
            .withColumn("_fichier", F.input_file_name())
            .withColumn("_lot", F.lit(LOT))
            .withColumn("_ingere_le", F.current_timestamp())
            .withColumn("date_ingestion", F.lit(DATE_INGESTION)))


SCHEMA_TRAJETS = StructType([
    StructField("trajet_id", StringType(), True),
    StructField("client_id", StringType(), True),
    StructField("engin_id", StringType(), True),
    StructField("type_engin", StringType(), True),
    StructField("ville", StringType(), True),
    StructField("station_depart", StringType(), True),
    StructField("station_arrivee", StringType(), True),
    StructField("debut", StringType(), True),      # volontairement en texte :
    StructField("fin", StringType(), True),        # 3 formats coexistent
    StructField("duree_min", IntegerType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("montant_eur", DoubleType(), True),
    StructField("moyen_paiement", StringType(), True),
])


def main():
    spark = spark_session("TP2-bronze")
    resume = []

    # ------------------------------------------------------------ 1. trajets
    trajets = (spark.read
               .schema(SCHEMA_TRAJETS)          # schema impose
               .option("header", True)
               .option("mode", "PERMISSIVE")    # lignes illisibles -> NULL, pas d'echec
               .csv(os.path.join(C["raw"], "trajets")))
    trajets = tracer(trajets, "trajets_csv")
    ecrire_parquet(trajets, os.path.join(C["bronze"], "trajets"), ["date_ingestion"])
    resume.append(("trajets", trajets.count()))

    # -------------------------------------------------------- 2. telemetrie
    # JSON Lines compresse : Spark decompresse gzip nativement.
    # Le schema est ici infere car la source est semi-structuree et evolutive
    # (champ "firmware" present sur 5 % des evenements seulement).
    telemetrie = spark.read.json(os.path.join(C["raw"], "telemetrie"))
    telemetrie = tracer(telemetrie, "telemetrie_iot")
    ecrire_parquet(telemetrie, os.path.join(C["bronze"], "telemetrie"),
                   ["date_ingestion"])
    resume.append(("telemetrie", telemetrie.count()))
    print("\nSchema infere de la telemetrie (source semi-structuree) :")
    telemetrie.printSchema()

    # ----------------------------------------------------------- 3. stations
    stations = (spark.read.option("header", True).option("inferSchema", True)
                .csv(os.path.join(C["raw"], "stations.csv")))
    stations = tracer(stations, "referentiel_stations")
    ecrire_parquet(stations, os.path.join(C["bronze"], "stations"))
    resume.append(("stations", stations.count()))

    # ------------------------------------------------------------ 4. clients
    # ATTENTION : donnees a caractere personnel. En bronze on conserve la source
    # telle quelle, MAIS l'acces au dossier doit etre restreint et la retention
    # limitee (cf. chapitre gouvernance). La pseudonymisation a lieu en silver.
    clients = (spark.read.option("header", True).option("inferSchema", True)
               .csv(os.path.join(C["raw"], "clients.csv")))
    clients = tracer(clients, "clients_crm")
    ecrire_parquet(clients, os.path.join(C["bronze"], "clients"))
    resume.append(("clients (DCP)", clients.count()))

    # -------------------------------------------------------- 5. maintenance
    # Tableaux JSON a schema variable : multiLine obligatoire.
    maintenance = (spark.read.option("multiLine", True)
                   .json(os.path.join(C["raw"], "maintenance")))
    maintenance = tracer(maintenance, "maintenance_tickets")
    ecrire_parquet(maintenance, os.path.join(C["bronze"], "maintenance"))
    resume.append(("maintenance", maintenance.count()))

    # -------------------------------------------------------------- 6. meteo
    # Source externe type API : le JSON contient un objet racine + un tableau.
    # explode() aplatit le tableau "releves" en lignes.
    brut = spark.read.option("multiLine", True).json(os.path.join(C["raw"], "meteo.json"))
    meteo = brut.select(F.explode("releves").alias("r")).select("r.*")
    meteo = tracer(meteo, "api_meteo")
    ecrire_parquet(meteo, os.path.join(C["bronze"], "meteo"))
    resume.append(("meteo", meteo.count()))

    # ---------------------------------------------------------------- resume
    print("\n=== Couche BRONZE ecrite ===")
    for nom, n in resume:
        print(f"  {nom:<16} {n:>10,} lignes")
    print(f"  lot d'ingestion : {LOT}")
    print(f"  emplacement     : {C['bronze']}")

    # -------------------------------------------------------- vos reponses
    # Les questions se repondent dans l'application, fiche « TP 2 - Ingestion,
    # qualite et conformite », section « Questions » : idempotence d'une
    # relance, role de la colonne _fichier, evolution de schema de la
    # telemetrie.

    spark.stop()


if __name__ == "__main__":
    main()
