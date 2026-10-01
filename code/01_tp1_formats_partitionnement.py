#!/usr/bin/env python3
"""
TP 1 (Jour 1) - Stockage, formats et partitionnement
====================================================

Objectif pedagogique (C27) : montrer, chiffres a l'appui, que le choix du format
de fichier et du partitionnement est une decision d'ARCHITECTURE, pas un detail
technique. On mesure trois choses sur le meme jeu de donnees :

  1. la taille sur disque   (cout de stockage)
  2. le temps de lecture    (cout de calcul)
  3. le volume reellement lu (data skipping / partition pruning)

A executer :  python 01_tp1_formats_partitionnement.py
"""

import os
import shutil
import time

from pyspark.sql import functions as F

from mobicity import chemins, spark_session, parse_horodatage, normaliser_ville

C = chemins()
BANC = os.path.join(C["racine"], "banc_essai")


def taille_mo(chemin: str) -> float:
    total = 0
    for racine, _, fichiers in os.walk(chemin):
        for f in fichiers:
            total += os.path.getsize(os.path.join(racine, f))
    return total / (1024 * 1024)


def nb_fichiers(chemin: str) -> int:
    return sum(len(f) for _, _, f in os.walk(chemin))


def chrono(libelle, fonction):
    t0 = time.perf_counter()
    resultat = fonction()
    duree = time.perf_counter() - t0
    print(f"  {libelle:<46} {duree:7.2f} s   -> {resultat}")
    return duree


def main():
    spark = spark_session("TP1-formats")

    # ---------------------------------------------------------------- etape 1
    print("\n[1] Lecture des trajets bruts (CSV)")
    trajets = (spark.read
               .option("header", True)
               .option("inferSchema", True)   # pratique en TP, a proscrire en prod
               .csv(os.path.join(C["raw"], "trajets")))
    print(f"    lignes = {trajets.count():,} | colonnes = {len(trajets.columns)}")
    trajets.printSchema()

    # -------------------------------------------------------- vos reponses
    # Les questions de ce TP se repondent DANS L'APPLICATION, fiche « TP 1 -
    # Stockage, formats et partitionnement », section « Questions » : un champ
    # par question, enregistre a la volee, relu par la formatrice.
    #
    # Les cinq questions y attendent une reponse ecrite, notamment le cout de
    # l'inference de schema, le facteur de compression que VOUS mesurez, et ce
    # qu'il advient si l'on partitionne par trajet_id.

    # ---------------------------------------------------------------- etape 2
    print("\n[2] Preparation minimale (dates, ville, colonnes de partition)")
    prepare = (trajets
               .withColumn("debut_ts", parse_horodatage("debut"))
               .withColumn("ville", normaliser_ville("ville"))
               .withColumn("annee", F.year("debut_ts"))
               .withColumn("mois", F.month("debut_ts"))
               .filter(F.col("debut_ts").isNotNull()))
    prepare.cache()
    n = prepare.count()
    print(f"    lignes exploitables = {n:,}")

    # ---------------------------------------------------------------- etape 3
    print("\n[3] Ecriture dans 4 configurations de stockage")
    if os.path.exists(BANC):
        # Windows : fichiers Spark staging / chemins longs peuvent disparaitre
        # pendant le parcours -> ignore_errors evite le FileNotFoundError.
        shutil.rmtree(BANC, ignore_errors=True)
    os.makedirs(BANC, exist_ok=True)

    configs = {
        "csv_simple":            lambda p: prepare.write.mode("overwrite").option("header", True).csv(p),
        "parquet_simple":        lambda p: prepare.write.mode("overwrite").parquet(p),
        "parquet_partitionne":   lambda p: prepare.write.mode("overwrite").partitionBy("ville", "mois").parquet(p),
        "parquet_trop_partitionne": lambda p: prepare.write.mode("overwrite").partitionBy("ville", "mois", "type_engin").parquet(p),
    }
    tailles = {}
    for nom, ecrire in configs.items():
        chemin = os.path.join(BANC, nom)
        t0 = time.perf_counter()
        ecrire(chemin)
        t = time.perf_counter() - t0
        tailles[nom] = (taille_mo(chemin), nb_fichiers(chemin), t)

    print(f"\n    {'configuration':<26}{'taille (Mo)':>12}{'fichiers':>10}{'ecriture':>11}")
    for nom, (mo, nbf, t) in tailles.items():
        print(f"    {nom:<26}{mo:>12.2f}{nbf:>10}{t:>10.2f}s")

    # ---------------------------------------------------------------- etape 4
    print("\n[4] Comparaison des temps de lecture d'une meme requete metier")
    print("    Requete : chiffre d'affaires des trajets de Lyon en mars")

    def requete(chemin, format_="parquet"):
        lecteur = (spark.read.option("header", True).option("inferSchema", True).csv(chemin)
                   if format_ == "csv" else spark.read.parquet(chemin))
        r = (lecteur.filter((F.col("ville") == "Lyon") & (F.col("mois") == 3))
             .agg(F.round(F.sum(F.col("montant_eur").cast("double")), 2).alias("ca"))
             .collect()[0]["ca"])
        return f"CA = {r} EUR"

    chrono("CSV (scan complet)",
           lambda: requete(os.path.join(BANC, "csv_simple"), "csv"))
    chrono("Parquet non partitionne (scan + filtre)",
           lambda: requete(os.path.join(BANC, "parquet_simple")))
    chrono("Parquet partitionne (partition pruning)",
           lambda: requete(os.path.join(BANC, "parquet_partitionne")))

    # ---------------------------------------------------------------- etape 5
    print("\n[5] Lecture du plan d'execution : ou voit-on le pruning ?")
    plan = (spark.read.parquet(os.path.join(BANC, "parquet_partitionne"))
            .filter((F.col("ville") == "Lyon") & (F.col("mois") == 3))
            .groupBy("type_engin").agg(F.count("*").alias("n")))
    plan.explain(mode="formatted")

    print("\nTP1 termine. Resultats de stockage dans :", BANC)
    spark.stop()


if __name__ == "__main__":
    main()
