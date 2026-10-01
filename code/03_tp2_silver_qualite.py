#!/usr/bin/env python3
"""
TP 2b (Jour 2) - Nettoyage, qualite et conformite : couche SILVER
=================================================================

Objectif pedagogique (C28, C29 et gouvernance) : transformer une matiere
premiere sale en un actif de donnees fiable, documente et conforme.

Ce que fait ce script :
  1. normalise les formats (dates, villes, types)
  2. deduplique
  3. met en quarantaine les lignes non conformes AU LIEU de les supprimer
  4. valide l'integrite referentielle (cles orphelines)
  5. pseudonymise les donnees personnelles (RGPD)
  6. produit un rapport de qualite JSON exploitable

A executer :  python 03_tp2_silver_qualite.py   (apres 02_tp2_bronze_ingestion.py)
"""

import os

from pyspark.sql import Window
from pyspark.sql import functions as F

from mobicity import (ControleQualite, chemins, ecrire_parquet,
                      normaliser_ville, parse_horodatage, spark_session)

C = chemins()
SEL_PSEUDO = "efrei-m2-bigdata-2026"   # en production : secret gere par un coffre


def main():
    spark = spark_session("TP2-silver")

    bronze_trajets = spark.read.parquet(os.path.join(C["bronze"], "trajets"))
    stations = spark.read.parquet(os.path.join(C["bronze"], "stations"))
    clients = spark.read.parquet(os.path.join(C["bronze"], "clients"))

    n_depart = bronze_trajets.count()
    print(f"[0] Bronze en entree : {n_depart:,} trajets")

    # ------------------------------------------------- 1. Normalisation
    print("[1] Normalisation des formats")
    t = (bronze_trajets
         .withColumn("debut_ts", parse_horodatage("debut"))
         .withColumn("fin_ts", parse_horodatage("fin"))
         .withColumn("ville", normaliser_ville("ville"))
         .withColumn("moyen_paiement",
                     F.when(F.trim(F.col("moyen_paiement")) == "", None)
                      .otherwise(F.lower(F.trim(F.col("moyen_paiement")))))
         .withColumn("date", F.to_date("debut_ts"))
         .withColumn("heure", F.hour("debut_ts"))
         .withColumn("jour_semaine", F.date_format("debut_ts", "EEEE")))

    # Duree recalculee depuis les horodatages : on ne fait pas confiance
    # a la colonne fournie, on la CONTROLE.
    t = t.withColumn("duree_calc_min",
                     F.round((F.col("fin_ts").cast("long") -
                              F.col("debut_ts").cast("long")) / 60.0, 1))

    # ------------------------------------------------- 2. Deduplication
    print("[2] Deduplication")
    fenetre = Window.partitionBy("trajet_id").orderBy(F.col("_ingere_le").desc())
    t = (t.withColumn("_rang", F.row_number().over(fenetre))
          .filter(F.col("_rang") == 1).drop("_rang"))
    n_dedup = t.count()
    print(f"    {n_depart - n_dedup:,} doublons supprimes -> {n_dedup:,} lignes")

    # ------------------------------------- 3. Integrite referentielle
    print("[3] Controle d'integrite referentielle")
    ids_stations = stations.select(F.col("station_id").alias("_st")).distinct()
    ids_clients = clients.select(F.col("client_id").alias("_cl")).distinct()

    t = (t.join(F.broadcast(ids_stations),
                t.station_arrivee == F.col("_st"), "left")
          .withColumn("station_arrivee_connue", F.col("_st").isNotNull()).drop("_st")
          .join(F.broadcast(ids_clients),
                t.client_id == F.col("_cl"), "left")
          .withColumn("client_connu", F.col("_cl").isNotNull()).drop("_cl"))

    # ------------------------------------ 4. Regles metier et quarantaine
    print("[4] Application des regles metier")
    regles_valide = (
        F.col("debut_ts").isNotNull() &
        F.col("fin_ts").isNotNull() &
        (F.col("duree_min") > 0) &
        (F.col("duree_min") < 600) &          # > 10 h = anomalie operationnelle
        (F.col("distance_km") > 0) &
        F.col("montant_eur").isNotNull() &
        (F.col("montant_eur") >= 0) &
        F.col("client_connu") &
        F.col("station_arrivee_connue")
    )

    # Le motif de rejet est explicite : indispensable pour corriger la source.
    motif = (F.when(F.col("debut_ts").isNull() | F.col("fin_ts").isNull(), "horodatage_illisible")
              .when(F.col("duree_min") <= 0, "duree_negative_ou_nulle")
              .when(F.col("duree_min") >= 600, "duree_aberrante")
              .when(F.col("distance_km") <= 0, "distance_nulle")
              .when(F.col("montant_eur").isNull(), "montant_manquant")
              .when(~F.col("client_connu"), "client_inconnu")
              .when(~F.col("station_arrivee_connue"), "station_inconnue")
              .otherwise("autre"))

    valides = t.filter(regles_valide)
    rejets = t.filter(~regles_valide | regles_valide.isNull()).withColumn("motif_rejet", motif)

    n_valides, n_rejets = valides.count(), rejets.count()
    print(f"    {n_valides:,} lignes valides | {n_rejets:,} en quarantaine "
          f"({n_rejets / n_dedup:.2%})")
    print("\n    Repartition des motifs de rejet :")
    rejets.groupBy("motif_rejet").count().orderBy(F.desc("count")).show(truncate=False)

    ecrire_parquet(rejets, os.path.join(C["quarantaine"], "trajets"), ["motif_rejet"])

    # ---------------------------------------- 5. RGPD : pseudonymisation
    print("[5] Pseudonymisation des donnees a caractere personnel")
    # Le client_id est remplace par un identifiant derive non reversible sans le sel.
    # On conserve la capacite d'analyse (comptage de clients uniques, recurrence)
    # sans exposer l'identifiant metier : minimisation des donnees.
    valides = valides.withColumn(
        "client_pseudo", F.sha2(F.concat_ws("|", F.col("client_id"), F.lit(SEL_PSEUDO)), 256))

    clients_silver = (clients
                      .withColumn("client_pseudo",
                                  F.sha2(F.concat_ws("|", F.col("client_id"), F.lit(SEL_PSEUDO)), 256))
                      .withColumn("tranche_age",
                                  F.when(F.floor(F.datediff(F.current_date(), F.to_date("date_naissance")) / 365.25) < 25, "18-24")
                                   .when(F.floor(F.datediff(F.current_date(), F.to_date("date_naissance")) / 365.25) < 35, "25-34")
                                   .when(F.floor(F.datediff(F.current_date(), F.to_date("date_naissance")) / 365.25) < 50, "35-49")
                                   .otherwise("50+"))
                      # On NE PROPAGE PAS nom, prenom, email, telephone, date de naissance
                      .select("client_pseudo", "tranche_age", "ville_residence",
                              "abonnement", "date_inscription", "consentement_marketing"))

    colonnes_silver = ["trajet_id", "client_pseudo", "engin_id", "type_engin", "ville",
                       "station_depart", "station_arrivee", "debut_ts", "fin_ts", "date",
                       "heure", "jour_semaine", "duree_min", "duree_calc_min",
                       "distance_km", "montant_eur", "moyen_paiement", "_lot"]
    trajets_silver = (valides.select(*colonnes_silver)
                      .withColumn("annee", F.year("date"))
                      .withColumn("mois", F.month("date")))

    # ------------------------------------------ 6. Controles qualite
    print("[6] Controles qualite sur la couche silver")
    cq = ControleQualite("trajets_silver", trajets_silver)
    (cq.regle("id_non_null", F.col("trajet_id").isNotNull(), 0.0,
              description="Chaque trajet possede un identifiant")
       .regle("duree_positive", F.col("duree_min") > 0, 0.0,
              description="Duree strictement positive")
       .regle("coherence_duree",
              F.abs(F.col("duree_min") - F.col("duree_calc_min")) <= 2, 0.05, bloquant=False,
              description="Duree declaree coherente avec les horodatages (+/- 2 min)")
       .regle("montant_positif", F.col("montant_eur") >= 0, 0.0,
              description="Montant jamais negatif")
       .regle("ville_referencee",
              F.col("ville").isin("Paris", "Lyon", "Marseille", "Lille", "Bordeaux", "Nantes"), 0.0,
              description="Ville appartenant au referentiel")
       .regle("pseudonymisation", F.length("client_pseudo") == 64, 0.0,
              description="Identifiant client pseudonymise (SHA-256)")
       .regle("fraicheur", F.col("date") >= F.lit("2026-01-01"), 0.0, bloquant=False,
              description="Donnees posterieures au debut de la periode"))
    rapport = cq.executer()
    cq.afficher()
    cq.sauver(os.path.join(C["racine"], "..", "logs", "qualite_trajets_silver.json"))

    if not rapport["conforme"]:
        print("!! Controles bloquants en echec : la couche gold ne doit pas etre publiee.")

    # --------------------------------------------------- 7. Ecriture
    ecrire_parquet(trajets_silver, os.path.join(C["silver"], "trajets"), ["annee", "mois"])
    ecrire_parquet(clients_silver, os.path.join(C["silver"], "clients"))
    ecrire_parquet(spark.read.parquet(os.path.join(C["bronze"], "stations"))
                   .select("station_id", "nom", "ville", "latitude", "longitude",
                           "capacite", "date_ouverture"),
                   os.path.join(C["silver"], "stations"))

    print(f"\n=== Couche SILVER ecrite dans {C['silver']} ===")
    print(f"  taux de rejet global : {n_rejets / n_dedup:.2%}")

    # -------------------------------------------------------- vos reponses
    # Les questions se repondent dans l'application, fiche « TP 2 - Ingestion,
    # qualite et conformite », section « Questions » : quarantaine plutot que
    # suppression, portee reelle de la pseudonymisation, regle de coherence du
    # montant, et qui fixe un seuil de qualite.

    spark.stop()


if __name__ == "__main__":
    main()
