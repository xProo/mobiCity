"""
Boite a outils commune aux TP du module Big Data (fil rouge MobiCity).

Contient :
  - la creation d'une SparkSession configuree pour un poste de travail,
  - la resolution des chemins de l'architecture medaillon (bronze / silver / gold),
  - le parsing des horodatages heterogenes,
  - un mini-framework de controles qualite reutilisable (regles + rapport JSON).

Import dans les scripts de TP :
    from mobicity import spark_session, chemins, parse_horodatage, ControleQualite
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# --------------------------------------------------------------------------
# Chemins
# --------------------------------------------------------------------------
RACINE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))


def chemins(racine: str | None = None) -> dict:
    """Renvoie les chemins des differentes couches du lakehouse local."""
    r = os.path.abspath(racine or RACINE)
    return {
        "racine": r,
        "raw": os.path.join(r, "raw"),
        "bronze": os.path.join(r, "bronze"),
        "silver": os.path.join(r, "silver"),
        "gold": os.path.join(r, "gold"),
        "quarantaine": os.path.join(r, "quarantaine"),
        "logs": os.path.join(r, "..", "logs"),
    }


# --------------------------------------------------------------------------
# Session Spark
# --------------------------------------------------------------------------
def spark_session(nom: str = "MobiCity", coeurs: str = "*",
                  memoire: str = "4g", ui: bool = True) -> SparkSession:
    """
    SparkSession locale pretes pour les TP.

    - master local[*] : Spark utilise tous les coeurs du poste, chaque coeur
      jouant le role d'un "executeur" => on illustre le parallelisme sans cluster.
    - shuffle.partitions abaisse a 8 : la valeur par defaut (200) est absurde
      sur un poste de travail et genere des centaines de petits fichiers.
    - AQE (Adaptive Query Execution) : Spark ajuste le plan a l'execution.
    """
    # Windows : winutils (HADOOP_HOME) requis pour lire/ecrire des fichiers locaux
    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        candidat = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "hadoop"))
        if os.path.isfile(os.path.join(candidat, "bin", "winutils.exe")):
            os.environ["HADOOP_HOME"] = candidat
            os.environ["PATH"] = os.path.join(candidat, "bin") + os.pathsep + os.environ.get("PATH", "")
    b = (SparkSession.builder
         .appName(nom)
         .master(f"local[{coeurs}]")
         .config("spark.driver.memory", memoire)
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.session.timeZone", "Europe/Paris")
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         # Windows : committer v1 + chemins de staging profonds -> AccessDenied
         .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
         .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true"))
    if not ui:
        b = b.config("spark.ui.enabled", "false")
    s = b.getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    return s


# --------------------------------------------------------------------------
# Parsing des horodatages heterogenes
# --------------------------------------------------------------------------
def parse_horodatage(colonne: str):
    """
    Les sources envoient trois formats de date differents (ISO, FR, epoch).
    On tente chaque format dans l'ordre ; si aucun ne marche, la valeur est NULL
    (et sera capturee par les controles qualite, jamais silencieusement ignoree).
    """
    c = F.col(colonne)
    try:  # Spark >= 3.5 : try_to_timestamp renvoie NULL au lieu de lever une erreur
        iso = F.try_to_timestamp(c, F.lit("yyyy-MM-dd HH:mm:ss"))
        fr = F.try_to_timestamp(c, F.lit("dd/MM/yyyy HH:mm"))
    except AttributeError:  # repli pour les versions plus anciennes
        iso = F.to_timestamp(c, "yyyy-MM-dd HH:mm:ss")
        fr = F.to_timestamp(c, "dd/MM/yyyy HH:mm")
    try:
        # Spark 3.5 : Column.try_cast ; Spark 4.x : l'attribut n'existe plus
        # et c.try_cast peut etre resolu comme champ struct (TypeError)
        epoch = F.timestamp_seconds(c.try_cast("long"))
    except (AttributeError, TypeError):
        epoch = F.timestamp_seconds(c.cast("long"))
    return F.coalesce(iso, fr, epoch)


def normaliser_ville(colonne: str):
    """Normalise les variantes de libelles de villes (casse, espaces, fautes)."""
    base = F.initcap(F.trim(F.lower(F.col(colonne))))
    return (F.when(base.isin("Pariss", "Pari"), "Paris")
             .when(base == "Marseile", "Marseille")
             .when(base == "Nante", "Nantes")
             .otherwise(base))


# --------------------------------------------------------------------------
# Mini-framework de controles qualite
# --------------------------------------------------------------------------
class ControleQualite:
    """
    Declare des regles de qualite sur un DataFrame, les execute en une passe
    et produit un rapport exploitable (console + JSON).

    Chaque regle est ATTENDUE VRAIE. Le taux de non-conformite est compare a un
    seuil : au-dela, la regle est bloquante (le pipeline doit s'arreter).

        cq = ControleQualite("trajets_silver", df)
        cq.regle("duree_positive", F.col("duree_min") > 0, seuil=0.02)
        cq.regle("client_connu", F.col("client_id").isNotNull(), seuil=0.0, bloquant=True)
        rapport = cq.executer()
        cq.afficher()
        cq.sauver("../logs/qualite_trajets.json")
        if not rapport["conforme"]:
            raise SystemExit("Controles qualite en echec")
    """

    def __init__(self, nom: str, df: DataFrame):
        self.nom = nom
        self.df = df
        self.regles: list[dict] = []
        self.rapport: dict | None = None

    def regle(self, nom: str, condition, seuil: float = 0.0,
              bloquant: bool = True, description: str = ""):
        self.regles.append({"nom": nom, "condition": condition, "seuil": seuil,
                            "bloquant": bloquant, "description": description})
        return self

    def executer(self) -> dict:
        total = self.df.count()
        # Une seule passe : on agrege tous les compteurs d'echec en meme temps.
        aggs = [F.sum(F.when(r["condition"], 0).otherwise(1)).alias(r["nom"])
                for r in self.regles]
        ligne = self.df.agg(*aggs).collect()[0].asDict() if self.regles else {}

        resultats, conforme = [], True
        for r in self.regles:
            echecs = int(ligne.get(r["nom"]) or 0)
            taux = (echecs / total) if total else 0.0
            ok = taux <= r["seuil"]
            if not ok and r["bloquant"]:
                conforme = False
            resultats.append({
                "regle": r["nom"], "description": r["description"],
                "lignes_en_echec": echecs, "taux": round(taux, 6),
                "seuil": r["seuil"], "bloquant": r["bloquant"], "statut": "OK" if ok else "ECHEC",
            })

        self.rapport = {
            "jeu": self.nom,
            "horodatage": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "lignes_controlees": total,
            "conforme": conforme,
            "resultats": resultats,
        }
        return self.rapport

    def afficher(self):
        r = self.rapport or self.executer()
        largeur = max([len(x["regle"]) for x in r["resultats"]] + [12])
        print(f"\n--- Qualite : {r['jeu']} ({r['lignes_controlees']:,} lignes) ---")
        for x in r["resultats"]:
            drapeau = "OK  " if x["statut"] == "OK" else "ECHEC"
            print(f"  [{drapeau}] {x['regle']:<{largeur}} "
                  f"echecs={x['lignes_en_echec']:>8,}  taux={x['taux']:.4%}  "
                  f"seuil={x['seuil']:.2%}{'  (bloquant)' if x['bloquant'] else ''}")
        print(f"  => jeu {'CONFORME' if r['conforme'] else 'NON CONFORME'}\n")
        return r

    def sauver(self, chemin: str):
        r = self.rapport or self.executer()
        os.makedirs(os.path.dirname(os.path.abspath(chemin)), exist_ok=True)
        with open(chemin, "w", encoding="utf-8") as f:
            json.dump(r, f, ensure_ascii=False, indent=2)
        return chemin


def ecrire_parquet(df: DataFrame, chemin: str, partitions: list[str] | None = None,
                   mode: str = "overwrite"):
    """Ecriture standardisee : Parquet + partitionnement optionnel."""
    w = df.write.mode(mode)
    if partitions:
        w = w.partitionBy(*partitions)
    w.parquet(chemin)
    return chemin
