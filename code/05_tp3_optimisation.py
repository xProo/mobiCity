#!/usr/bin/env python3
"""
TP 3b (Jour 3) - Traitement distribue : comprendre et optimiser
================================================================

Objectif pedagogique (C29) : rendre visible ce que Spark fait reellement.
Tant qu'on ne lit pas le plan d'execution, optimiser releve de la superstition.

Les 5 experiences :
  A. transformations paresseuses vs actions
  B. le shuffle : ce qui coute vraiment cher
  C. broadcast join vs shuffle join
  D. cache : quand il aide, quand il ne sert a rien
  E. partition pruning et projection (ne lire que le necessaire)

A executer :  python 05_tp3_optimisation.py
"""

import os
import time

from pyspark.sql import functions as F

from mobicity import chemins, spark_session

C = chemins()


def chrono(libelle, fonction):
    t0 = time.perf_counter()
    r = fonction()
    d = time.perf_counter() - t0
    print(f"    {libelle:<48} {d:7.2f} s   {r if r is not None else ''}")
    return d


def main():
    spark = spark_session("TP3-optimisation")
    trajets = spark.read.parquet(os.path.join(C["silver"], "trajets"))
    stations = spark.read.parquet(os.path.join(C["silver"], "stations"))
    telemetrie = spark.read.parquet(os.path.join(C["bronze"], "telemetrie"))

    # ================================================== A. Paresse (lazy)
    print("\n[A] Transformations paresseuses vs actions")
    t0 = time.perf_counter()
    df = (trajets.filter(F.col("ville") == "Paris")
          .withColumn("ca_ht", F.col("montant_eur") / 1.2)
          .groupBy("type_engin").agg(F.sum("ca_ht").alias("ca_ht")))
    print(f"    definition de 3 transformations : {time.perf_counter() - t0:.4f} s "
          "(rien n'a ete calcule)")
    chrono("premiere action (collect)", lambda: f"{len(df.collect())} lignes")

    # -------------------------------------------------------- vos reponses
    # Les questions se repondent dans l'application, fiche « TP 3 - Couche gold
    # et optimisation », sous l'exercice « Cinq mesures, et trois optimisations
    # qu'on peut prouver » : evaluation paresseuse, cout d'un groupBy a forte
    # cardinalite, limites de la diffusion et du cache.
    # La comparaison PartitionFilters / PushedFilters est posee au TP 1 : la
    # meme reponse vaut ici, sur un autre plan d'execution.

    # ================================================== B. Le shuffle
    print("\n[B] Le cout du shuffle")
    print(f"    partitions en entree : {trajets.rdd.getNumPartitions()}")

    sans_shuffle = trajets.select("montant_eur").filter(F.col("montant_eur") > 5)
    avec_shuffle = trajets.groupBy("client_pseudo").agg(F.sum("montant_eur").alias("total"))

    chrono("narrow  : filter + select (aucun echange)",
           lambda: f"{sans_shuffle.count():,} lignes")
    chrono("wide    : groupBy sur une cle a forte cardinalite",
           lambda: f"{avec_shuffle.count():,} groupes")
    chrono("wide    : groupBy sur une cle a faible cardinalite",
           lambda: f"{trajets.groupBy('ville').count().count()} groupes")

    print("\n    Plan du groupBy (reperer 'Exchange hashpartitioning') :")
    avec_shuffle.explain(mode="simple")

    # ================================================== C. Jointures
    print("\n[C] Broadcast join vs shuffle join")
    # stations est un petit referentiel : le diffuser a tous les executeurs
    # evite de redistribuer la grande table.
    sans_bc = (trajets.join(stations.withColumnRenamed("ville", "ville_station"),
                            trajets.station_depart == stations.station_id)
               .groupBy("ville_station").count())
    avec_bc = (trajets.join(F.broadcast(stations.withColumnRenamed("ville", "ville_station")),
                            trajets.station_depart == stations.station_id)
               .groupBy("ville_station").count())

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)   # on desactive l'auto
    chrono("jointure avec shuffle (broadcast desactive)", lambda: sans_bc.count())
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)
    chrono("jointure avec broadcast explicite", lambda: avec_bc.count())

    print("\n    Plan avec broadcast :")
    avec_bc.explain(mode="simple")

    # ================================================== D. Cache
    print("\n[D] Cache : utile seulement si le DataFrame est relu")
    # ATTENTION : ne pas mesurer avec count() seul. Sur du Parquet, Spark repond
    # a count() en lisant uniquement les metadonnees des fichiers : la mesure
    # serait faussee. On force donc un vrai calcul sur les colonnes.
    base = telemetrie.filter(F.col("batterie_pct").between(0, 100))

    def calcul():
        r = base.agg(F.avg("batterie_pct").alias("b"),
                     F.avg("vitesse_kmh").alias("v")).collect()[0]
        return f"batterie_moy={r['b']:.1f}"

    chrono("sans cache - passe 1", calcul)
    chrono("sans cache - passe 2", calcul)
    base.cache()
    chrono("avec cache - passe 1 (materialisation)", calcul)
    chrono("avec cache - passe 2 (lecture memoire)", calcul)
    base.unpersist()

    # ================================================== E. Ne lire que l'utile
    print("\n[E] Partition pruning + projection")
    chemin = os.path.join(C["silver"], "trajets")

    chrono("SELECT * sur toutes les partitions",
           lambda: spark.read.parquet(chemin).count())
    chrono("2 colonnes, 1 mois (pruning + projection)",
           lambda: (spark.read.parquet(chemin)
                    .filter((F.col("annee") == 2026) & (F.col("mois") == 3))
                    .select("montant_eur", "ville").count()))

    print("\n    Plan de la requete ciblee :")
    (spark.read.parquet(chemin)
     .filter((F.col("annee") == 2026) & (F.col("mois") == 3))
     .select("montant_eur", "ville")
     .explain(mode="formatted"))

    print("\n[F] Interface web Spark (a explorer pendant l'execution)")
    print("    -> http://localhost:4040  onglets Jobs / Stages / SQL / Storage")
    print("    Le script attend 45 s pour vous laisser regarder. Ctrl-C pour passer.")
    try:
        time.sleep(45)
    except KeyboardInterrupt:
        pass

    spark.stop()


if __name__ == "__main__":
    main()
