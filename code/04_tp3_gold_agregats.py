#!/usr/bin/env python3
"""
TP 3a (Jour 3) - Couche GOLD : des donnees propres aux indicateurs de decision
==============================================================================

Objectif pedagogique (C29, C30) : la couche gold ne contient plus des donnees,
elle contient des REPONSES A DES QUESTIONS METIER. Chaque table gold doit
pouvoir etre justifiee par la question qu'elle sert.

Questions du comite de direction MobiCity :
  Q1. Comment evoluent le chiffre d'affaires et l'usage, par ville et par jour ?
  Q2. Quelles stations sont saturees ou sous-utilisees ?
  Q3. Quel est le profil d'usage par type d'engin et par tranche horaire ?
  Q4. La meteo explique-t-elle les variations de frequentation ?
  Q5. Quels engins presentent un risque de panne (batterie + maintenance) ?

A executer :  python 04_tp3_gold_agregats.py   (apres 03_tp2_silver_qualite.py)
"""

import os

from pyspark.sql import functions as F

from mobicity import chemins, ecrire_parquet, spark_session

C = chemins()


def main():
    spark = spark_session("TP3-gold")

    trajets = spark.read.parquet(os.path.join(C["silver"], "trajets"))
    stations = spark.read.parquet(os.path.join(C["silver"], "stations"))
    clients = spark.read.parquet(os.path.join(C["silver"], "clients"))
    meteo = spark.read.parquet(os.path.join(C["bronze"], "meteo"))
    telemetrie = spark.read.parquet(os.path.join(C["bronze"], "telemetrie"))
    maintenance = spark.read.parquet(os.path.join(C["bronze"], "maintenance"))

    trajets.cache()
    print(f"Trajets silver : {trajets.count():,}")

    # ---------------------------------------------------------------- Q1
    # kpi_journalier : la table de reference du tableau de bord.
    print("[Q1] KPI journaliers par ville")
    kpi_jour = (trajets.groupBy("date", "ville")
                .agg(F.count("*").alias("nb_trajets"),
                     F.countDistinct("client_pseudo").alias("clients_actifs"),
                     F.round(F.sum("montant_eur"), 2).alias("ca_eur"),
                     F.round(F.avg("montant_eur"), 2).alias("panier_moyen_eur"),
                     F.round(F.avg("duree_min"), 1).alias("duree_moy_min"),
                     F.round(F.sum("distance_km"), 1).alias("distance_totale_km"),
                     F.countDistinct("engin_id").alias("engins_utilises"))
                .withColumn("ca_par_engin",
                            F.round(F.col("ca_eur") / F.col("engins_utilises"), 2)))
    ecrire_parquet(kpi_jour, os.path.join(C["gold"], "kpi_journalier"), ["ville"])
    kpi_jour.orderBy(F.desc("date")).show(5, truncate=False)

    # ---------------------------------------------------------------- Q2
    print("[Q2] Performance des stations")
    # PIEGE PEDAGOGIQUE : sur la periode complete, arrivees et departs
    # s'equilibrent presque partout -> l'indicateur global ne montre RIEN.
    # Le desequilibre n'apparait qu'a la bonne granularite temporelle
    # (heures de pointe du matin). Le choix de la maille est un choix d'analyse.
    # Astuce : on renomme la cle de jointure dans chaque agregat ("station_id")
    # pour eviter les colonnes ambigues lors des jointures successives.
    def par_station(df, colonne, alias):
        return (df.groupBy(F.col(colonne).alias("station_id"))
                .agg(F.count("*").alias(alias)))

    matin = F.col("heure").between(7, 9)
    departs = par_station(trajets, "station_depart", "nb_departs")
    arrivees = par_station(trajets, "station_arrivee", "nb_arrivees")
    dep_matin = par_station(trajets.filter(matin), "station_depart", "departs_matin")
    arr_matin = par_station(trajets.filter(matin), "station_arrivee", "arrivees_matin")

    perf_stations = (stations
                     .join(departs, "station_id", "left")
                     .join(arrivees, "station_id", "left")
                     .join(dep_matin, "station_id", "left")
                     .join(arr_matin, "station_id", "left")
                     .fillna(0, ["nb_departs", "nb_arrivees", "departs_matin", "arrivees_matin"])
                     .withColumn("flux_net_global", F.col("nb_arrivees") - F.col("nb_departs"))
                     .withColumn("flux_net_matin",
                                 F.col("arrivees_matin") - F.col("departs_matin"))
                     .withColumn("taux_remplissage_matin",
                                 F.round(F.col("flux_net_matin") / F.col("capacite"), 2))
                     .withColumn("rotation",
                                 F.round((F.col("nb_departs") + F.col("nb_arrivees")) /
                                         F.col("capacite"), 2))
                     .withColumn("alerte",
                                 F.when(F.col("taux_remplissage_matin") > 1.5, "saturation_matin")
                                  .when(F.col("taux_remplissage_matin") < -1.5, "penurie_matin")
                                  .otherwise("equilibre"))
                     .select("station_id", "nom", "ville", "latitude", "longitude",
                             "capacite", "nb_departs", "nb_arrivees", "flux_net_global",
                             "departs_matin", "arrivees_matin", "flux_net_matin",
                             "taux_remplissage_matin", "rotation", "alerte"))
    ecrire_parquet(perf_stations, os.path.join(C["gold"], "performance_stations"))
    print("    Flux net GLOBAL (periode complete) : l'indicateur est muet")
    (perf_stations.agg(F.round(F.avg("flux_net_global"), 2).alias("flux_net_global_moyen"),
                       F.round(F.avg(F.abs("flux_net_matin")), 2).alias("ecart_matin_moyen"))
     .show())
    perf_stations.groupBy("alerte").count().orderBy(F.desc("count")).show()
    (perf_stations.orderBy(F.desc("flux_net_matin"))
     .select("nom", "ville", "capacite", "flux_net_matin", "taux_remplissage_matin", "alerte")
     .show(5, truncate=False))

    # ---------------------------------------------------------------- Q3
    print("[Q3] Profil d'usage horaire par type d'engin")
    usage_horaire = (trajets
                     .withColumn("tranche",
                                 F.when(F.col("heure").between(6, 9), "matin_pointe")
                                  .when(F.col("heure").between(10, 15), "journee")
                                  .when(F.col("heure").between(16, 19), "soir_pointe")
                                  .otherwise("nuit"))
                     .groupBy("ville", "type_engin", "tranche")
                     .agg(F.count("*").alias("nb_trajets"),
                          F.round(F.avg("duree_min"), 1).alias("duree_moy"),
                          F.round(F.sum("montant_eur"), 2).alias("ca_eur")))
    ecrire_parquet(usage_horaire, os.path.join(C["gold"], "usage_horaire"))

    # Segmentation client (jointure avec le referentiel pseudonymise)
    segments = (trajets.join(F.broadcast(clients), "client_pseudo", "left")
                .groupBy("abonnement", "tranche_age")
                .agg(F.countDistinct("client_pseudo").alias("clients"),
                     F.count("*").alias("nb_trajets"),
                     F.round(F.sum("montant_eur"), 2).alias("ca_eur"))
                .withColumn("trajets_par_client",
                            F.round(F.col("nb_trajets") / F.col("clients"), 2)))
    ecrire_parquet(segments, os.path.join(C["gold"], "segments_clients"))
    segments.orderBy(F.desc("ca_eur")).show(8)

    # ---------------------------------------------------------------- Q4
    print("[Q4] Correlation frequentation / meteo")
    meteo_j = meteo.select(F.to_date("date").alias("date"), "ville",
                           "temp_c", "pluie_mm", "vent_kmh")
    freq_meteo = (kpi_jour.join(meteo_j, ["date", "ville"], "left")
                  .withColumn("meteo_categorie",
                              F.when(F.col("pluie_mm") > 3, "pluvieux")
                               .when(F.col("temp_c") > 22, "chaud")
                               .when(F.col("temp_c") < 8, "froid")
                               .otherwise("normal")))
    ecrire_parquet(freq_meteo, os.path.join(C["gold"], "frequentation_meteo"))
    (freq_meteo.groupBy("meteo_categorie")
     .agg(F.round(F.avg("nb_trajets"), 1).alias("trajets_moy_jour"),
          F.round(F.avg("ca_eur"), 2).alias("ca_moy_jour"))
     .orderBy(F.desc("trajets_moy_jour")).show())

    # ---------------------------------------------------------------- Q5
    print("[Q5] Sante de la flotte (telemetrie + maintenance)")
    sante = (telemetrie
             .filter(F.col("batterie_pct").between(0, 100))   # bornes physiques
             .groupBy("engin_id")
             .agg(F.round(F.avg("batterie_pct"), 1).alias("batterie_moy"),
                  F.min("batterie_pct").alias("batterie_min"),
                  F.count("*").alias("nb_mesures"),
                  F.round(F.avg("vitesse_kmh"), 1).alias("vitesse_moy"),
                  F.sum(F.when(F.col("statut") == "hors_service", 1)
                        .otherwise(0)).alias("mesures_hors_service")))
    tickets = (maintenance.groupBy("engin_id")
               .agg(F.count("*").alias("nb_tickets"),
                    F.sum(F.when(F.col("priorite") == "P1", 1).otherwise(0)).alias("tickets_p1")))
    sante = (sante.join(tickets, "engin_id", "left").fillna(0, ["nb_tickets", "tickets_p1"])
             .withColumn("score_risque",
                         F.round(F.least(F.lit(100),
                                 (100 - F.col("batterie_moy")) * 0.4 +
                                 F.col("nb_tickets") * 8 +
                                 F.col("tickets_p1") * 15 +
                                 (F.col("mesures_hors_service") /
                                  F.col("nb_mesures") * 100) * 0.3), 1))
             .withColumn("priorite_intervention",
                         F.when(F.col("score_risque") >= 60, "urgente")
                          .when(F.col("score_risque") >= 35, "planifiee")
                          .otherwise("aucune")))
    ecrire_parquet(sante, os.path.join(C["gold"], "sante_flotte"))
    sante.groupBy("priorite_intervention").count().show()

    print(f"\n=== Couche GOLD ecrite dans {C['gold']} ===")
    for table in sorted(os.listdir(C["gold"])):
        print(f"  - {table}")

    # -------------------------------------------------------- vos reponses
    # Les questions se repondent dans l'application, fiche « TP 3 - Couche gold
    # et optimisation », sous l'exercice « Six tables gold, cinq questions de
    # direction » : justification des ponderations du score de risque, cle de
    # partitionnement de kpi_journalier, et ce que l'on doit dire d'un chiffre
    # qui exclut la quarantaine.

    spark.stop()


if __name__ == "__main__":
    main()
