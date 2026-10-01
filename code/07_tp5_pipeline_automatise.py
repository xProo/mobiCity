#!/usr/bin/env python3
"""
TP 5 (Jour 3) - Interface programmee d'automatisation  [modele pour E5 - Partie 2]
==================================================================================

Objectif pedagogique (C31) : automatiser une chaine de traitement data de bout
en bout, avec le niveau d'exigence attendu en production.

Un script qui "marche quand on le lance a la main" n'est PAS une automatisation.
Une automatisation doit repondre a six questions :

  1. Rejouabilite   : que se passe-t-il si je la relance deux fois ? (idempotence)
  2. Robustesse     : que fait-elle quand une etape echoue ? (retry, arret propre)
  3. Tracabilite    : qui a lance quoi, quand, sur quelles donnees, avec quel resultat ?
  4. Controle       : publie-t-elle des donnees non conformes ? (portes qualite)
  5. Alerte         : qui est prevenu, et avec quelle information utile ?
  6. Exploitation   : comment la relancer, la suspendre, la faire evoluer ?

Utilisation :
    python 07_tp5_pipeline_automatise.py                 # execution complete
    python 07_tp5_pipeline_automatise.py --depuis gold   # reprise apres incident
    python 07_tp5_pipeline_automatise.py --seulement qualite
    python 07_tp5_pipeline_automatise.py --simulation    # dry-run, rien n'est ecrit
    python 07_tp5_pipeline_automatise.py --forcer        # ignore l'etat precedent

Codes de sortie (indispensables pour un ordonnanceur) :
    0 = succes | 1 = echec technique | 2 = echec qualite bloquant
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

BASE = os.path.dirname(os.path.abspath(__file__))
RACINE = os.path.abspath(os.path.join(BASE, ".."))
DOSSIER_LOGS = os.path.join(RACINE, "logs")
DOSSIER_DATA = os.path.join(RACINE, "data")
JOURNAL = os.path.join(DOSSIER_LOGS, "journal_executions.jsonl")
ETAT = os.path.join(DOSSIER_LOGS, "etat_pipeline.json")
ALERTES = os.path.join(DOSSIER_LOGS, "alertes.log")

os.makedirs(DOSSIER_LOGS, exist_ok=True)

# --------------------------------------------------------------------------
# 1. Journalisation : console lisible + fichier structure
# --------------------------------------------------------------------------
ID_EXECUTION = datetime.now().strftime("%Y%m%d-%H%M%S")

logger = logging.getLogger("pipeline")
logger.setLevel(logging.INFO)
_console = logging.StreamHandler(sys.stdout)
_console.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-7s  %(message)s",
                                        datefmt="%H:%M:%S"))
_fichier = logging.FileHandler(os.path.join(DOSSIER_LOGS, f"pipeline_{ID_EXECUTION}.log"),
                               encoding="utf-8")
_fichier.setFormatter(logging.Formatter(
    "%(asctime)s | %(levelname)s | " + ID_EXECUTION + " | %(message)s"))
logger.addHandler(_console)
logger.addHandler(_fichier)


def journaliser(evenement: dict):
    """Ecrit une ligne JSON dans le journal d'executions (exploitable par un outil)."""
    evenement = {"execution_id": ID_EXECUTION,
                 "horodatage": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                 **evenement}
    with open(JOURNAL, "a", encoding="utf-8") as f:
        f.write(json.dumps(evenement, ensure_ascii=False) + "\n")


def alerter(niveau: str, message: str, contexte: dict | None = None):
    """
    Point unique de notification.
    En production on brancherait ici un envoi SMTP, un webhook Teams/Slack ou la
    creation d'un ticket. On ecrit dans un fichier pour rester hors ligne en TP.
    """
    ligne = (f"[{datetime.now().isoformat(timespec='seconds')}] {niveau} | "
             f"{message} | {json.dumps(contexte or {}, ensure_ascii=False)}")
    with open(ALERTES, "a", encoding="utf-8") as f:
        f.write(ligne + "\n")
    logger.warning("ALERTE %s : %s", niveau, message)


# --------------------------------------------------------------------------
# 2. Idempotence : empreinte des donnees sources
# --------------------------------------------------------------------------
def empreinte_sources() -> str:
    """
    Signature du contenu du dossier raw (noms + tailles + dates de modification).
    Si l'empreinte n'a pas change, retraiter ne produirait rien de nouveau :
    le pipeline peut s'arreter tot et economiser des ressources.
    """
    h = hashlib.sha256()
    raw = os.path.join(DOSSIER_DATA, "raw")
    for racine, _, fichiers in sorted(os.walk(raw)):
        for nom in sorted(fichiers):
            chemin = os.path.join(racine, nom)
            st = os.stat(chemin)
            h.update(f"{os.path.relpath(chemin, raw)}|{st.st_size}|{int(st.st_mtime)}".encode())
    return h.hexdigest()[:16]


def lire_etat() -> dict:
    if os.path.exists(ETAT):
        with open(ETAT, encoding="utf-8") as f:
            return json.load(f)
    return {}


def ecrire_etat(etat: dict):
    with open(ETAT, "w", encoding="utf-8") as f:
        json.dump(etat, f, ensure_ascii=False, indent=2)


# --------------------------------------------------------------------------
# 3. Execution d'une etape avec reprise sur erreur
# --------------------------------------------------------------------------
class EchecEtape(Exception):
    pass


def executer_etape(nom: str, script: str, tentatives: int = 2,
                   attente: int = 10, simulation: bool = False) -> dict:
    """
    Lance un script de traitement en sous-processus.

    - tentatives  : nombre d'essais (une panne reseau ou un verrou est souvent
                    transitoire ; une erreur de code ne l'est jamais).
    - attente     : temporisation croissante entre deux essais (backoff).
    - le code retour du sous-processus fait foi.
    """
    logger.info("Etape '%s' : demarrage (%s)", nom, script)
    if simulation:
        logger.info("Etape '%s' : SIMULATION, aucune ecriture", nom)
        return {"etape": nom, "statut": "simule", "duree_s": 0}

    derniere_erreur = ""
    for essai in range(1, tentatives + 1):
        t0 = time.perf_counter()
        proc = subprocess.run([sys.executable, os.path.join(BASE, script)],
                              capture_output=True, text=True, cwd=BASE)
        duree = round(time.perf_counter() - t0, 1)

        if proc.returncode == 0:
            logger.info("Etape '%s' : succes en %.1f s (essai %d)", nom, duree, essai)
            resultat = {"etape": nom, "statut": "succes", "duree_s": duree,
                        "essais": essai, "script": script}
            journaliser(resultat)
            return resultat

        derniere_erreur = (proc.stderr or proc.stdout or "")[-1500:]
        logger.error("Etape '%s' : echec (essai %d/%d, code %d)",
                     nom, essai, tentatives, proc.returncode)
        journaliser({"etape": nom, "statut": "echec", "duree_s": duree,
                     "essais": essai, "code_retour": proc.returncode,
                     "erreur": derniere_erreur[-500:]})
        if essai < tentatives:
            logger.info("Nouvelle tentative dans %d s", attente * essai)
            time.sleep(attente * essai)

    alerter("CRITIQUE", f"Etape '{nom}' en echec definitif",
            {"script": script, "extrait_erreur": derniere_erreur[-300:]})
    raise EchecEtape(f"{nom} : {derniere_erreur[-300:]}")


# --------------------------------------------------------------------------
# 4. Porte qualite : le pipeline refuse de publier des donnees non conformes
# --------------------------------------------------------------------------
def porte_qualite() -> dict:
    """Relit le rapport produit par l'etape silver et decide de continuer ou non."""
    chemin = os.path.join(DOSSIER_LOGS, "qualite_trajets_silver.json")
    if not os.path.exists(chemin):
        raise EchecEtape("Rapport qualite absent : impossible de valider la publication")

    with open(chemin, encoding="utf-8") as f:
        rapport = json.load(f)

    echecs = [r for r in rapport["resultats"] if r["statut"] == "ECHEC"]
    bloquants = [r for r in echecs if r["bloquant"]]

    logger.info("Porte qualite : %d regles, %d en echec dont %d bloquantes",
                len(rapport["resultats"]), len(echecs), len(bloquants))
    journaliser({"etape": "porte_qualite", "statut": "ok" if not bloquants else "bloque",
                 "regles_en_echec": [r["regle"] for r in echecs],
                 "lignes_controlees": rapport["lignes_controlees"]})

    if echecs and not bloquants:
        alerter("AVERTISSEMENT", "Regles qualite non bloquantes en echec",
                {"regles": [r["regle"] for r in echecs]})
    if bloquants:
        alerter("CRITIQUE", "Publication interrompue : qualite non conforme",
                {"regles": [r["regle"] for r in bloquants]})
    return {"conforme": not bloquants, "rapport": rapport}


# --------------------------------------------------------------------------
# 5. Restitution automatique : export du rapport quotidien
# --------------------------------------------------------------------------
def exporter_restitution() -> list[str]:
    """
    Produit les livrables destines aux equipes metier :
      - un CSV des stations a rebalancer (usage terrain)
      - une synthese HTML autoportante (usage direction)
    Automatiser la restitution, c'est supprimer la tache repetitive
    "extraire, mettre en forme, envoyer" faite chaque matin a la main.
    """
    import duckdb

    gold = os.path.join(DOSSIER_DATA, "gold")
    sortie = os.path.join(RACINE, "restitution")
    os.makedirs(sortie, exist_ok=True)
    jour = datetime.now().strftime("%Y-%m-%d")

    def lire(table):
        return (f"read_parquet('{os.path.join(gold, table)}/**/*.parquet', "
                f"hive_partitioning=true)")

    con = duckdb.connect()
    stations = con.execute(f"""
        SELECT nom, ville, capacite, departs_matin, arrivees_matin,
               flux_net_matin, alerte
        FROM {lire('performance_stations')}
        WHERE alerte <> 'equilibre'
        ORDER BY abs(flux_net_matin) DESC
    """).fetchdf()
    csv = os.path.join(sortie, f"stations_a_rebalancer_{jour}.csv")
    stations.to_csv(csv, index=False, encoding="utf-8-sig", sep=";")

    synthese = con.execute(f"""
        SELECT ville,
               sum(nb_trajets)                 AS trajets,
               round(sum(ca_eur), 2)           AS ca_eur,
               round(avg(panier_moyen_eur), 2) AS panier_moyen
        FROM {lire('kpi_journalier')}
        GROUP BY 1 ORDER BY ca_eur DESC
    """).fetchdf()
    flotte = con.execute(f"""
        SELECT count(*) AS engins_urgents FROM {lire('sante_flotte')}
        WHERE priorite_intervention = 'urgente'
    """).fetchdf().iloc[0]["engins_urgents"]
    con.close()

    html = f"""<!DOCTYPE html><html lang="fr"><head><meta charset="utf-8">
<title>MobiCity - synthese du {jour}</title>
<style>
 body{{font-family:system-ui,-apple-system,Segoe UI,sans-serif;margin:2rem auto;max-width:900px;color:#212529}}
 h1{{font-size:1.4rem}} table{{border-collapse:collapse;width:100%;margin:1rem 0}}
 th,td{{border-bottom:1px solid #dee2e6;padding:.5rem;text-align:right}}
 th:first-child,td:first-child{{text-align:left}} th{{background:#f1f3f5}}
 .note{{color:#868e96;font-size:.85rem}} .alerte{{color:#e03131;font-weight:600}}
</style></head><body>
<h1>MobiCity - synthese automatique du {jour}</h1>
<p>Genere par le pipeline <code>{ID_EXECUTION}</code> a partir de la couche gold.</p>
<h2>Activite par ville</h2>
{synthese.to_html(index=False, border=0)}
<h2>Exploitation</h2>
<p><span class="alerte">{int(flotte)} engins</span> necessitent une intervention urgente.
{len(stations)} stations presentent un desequilibre matinal
(detail dans <code>{os.path.basename(csv)}</code>).</p>
<p class="note">Perimetre : trajets conformes uniquement. Les trajets ecartes par les
controles qualite sont conserves en quarantaine et ne sont pas comptabilises ici.
Donnees clients pseudonymisees.</p>
</body></html>"""
    page = os.path.join(sortie, f"synthese_{jour}.html")
    with open(page, "w", encoding="utf-8") as f:
        f.write(html)

    logger.info("Restitution exportee : %s | %s", os.path.basename(csv),
                os.path.basename(page))
    journaliser({"etape": "restitution", "statut": "succes",
                 "fichiers": [os.path.basename(csv), os.path.basename(page)]})
    return [csv, page]


# --------------------------------------------------------------------------
# 6. Orchestration
# --------------------------------------------------------------------------
ETAPES = [
    ("ingestion", "02_tp2_bronze_ingestion.py"),
    ("preparation", "03_tp2_silver_qualite.py"),
    ("qualite", None),                       # porte qualite (pas un script)
    ("indicateurs", "04_tp3_gold_agregats.py"),
    ("restitution", None),                   # export des livrables
]


def main() -> int:
    p = argparse.ArgumentParser(description="Pipeline automatise MobiCity")
    p.add_argument("--depuis", choices=[e[0] for e in ETAPES],
                   help="reprendre a partir de cette etape")
    p.add_argument("--seulement", choices=[e[0] for e in ETAPES],
                   help="n'executer que cette etape")
    p.add_argument("--simulation", action="store_true", help="dry-run")
    p.add_argument("--forcer", action="store_true",
                   help="executer meme si les sources n'ont pas change")
    p.add_argument("--tentatives", type=int, default=2)
    args = p.parse_args()

    debut = time.perf_counter()
    logger.info("=" * 64)
    logger.info("Pipeline MobiCity | execution %s", ID_EXECUTION)

    # --- controle d'idempotence -------------------------------------------
    etat = lire_etat()
    empreinte = empreinte_sources()
    logger.info("Empreinte des sources : %s (precedente : %s)",
                empreinte, etat.get("empreinte_sources", "aucune"))
    if (empreinte == etat.get("empreinte_sources")
            and etat.get("dernier_statut") == "succes"
            and not args.forcer and not args.seulement and not args.depuis):
        logger.info("Sources inchangees depuis la derniere execution reussie. "
                    "Rien a faire (utiliser --forcer pour retraiter).")
        journaliser({"etape": "pipeline", "statut": "ignore",
                     "motif": "sources_inchangees"})
        return 0

    # --- selection des etapes ---------------------------------------------
    etapes = ETAPES
    if args.seulement:
        etapes = [e for e in ETAPES if e[0] == args.seulement]
    elif args.depuis:
        index = [e[0] for e in ETAPES].index(args.depuis)
        etapes = ETAPES[index:]

    resume, code_sortie = [], 0
    try:
        for nom, script in etapes:
            if nom == "qualite":
                if args.simulation:
                    logger.info("Etape 'qualite' : SIMULATION")
                    continue
                resultat = porte_qualite()
                resume.append({"etape": "qualite",
                               "statut": "succes" if resultat["conforme"] else "bloque"})
                if not resultat["conforme"]:
                    logger.error("Arret : les donnees ne sont pas publiables.")
                    code_sortie = 2
                    break
            elif nom == "restitution":
                if args.simulation:
                    logger.info("Etape 'restitution' : SIMULATION")
                    continue
                exporter_restitution()
                resume.append({"etape": "restitution", "statut": "succes"})
            else:
                resume.append(executer_etape(nom, script, tentatives=args.tentatives,
                                             simulation=args.simulation))
    except EchecEtape as e:
        logger.error("Pipeline interrompu : %s", e)
        code_sortie = 1

    duree = round(time.perf_counter() - debut, 1)
    statut = {0: "succes", 1: "echec_technique", 2: "echec_qualite"}[code_sortie]

    if not args.simulation:
        ecrire_etat({"derniere_execution": ID_EXECUTION,
                     "dernier_statut": statut,
                     "empreinte_sources": empreinte if code_sortie == 0 else etat.get("empreinte_sources"),
                     "duree_s": duree,
                     "horodatage": datetime.now(timezone.utc).isoformat(timespec="seconds")})
    journaliser({"etape": "pipeline", "statut": statut, "duree_s": duree,
                 "etapes": resume})

    logger.info("-" * 64)
    logger.info("Statut final : %s en %.1f s", statut.upper(), duree)
    logger.info("Journal : %s", JOURNAL)
    logger.info("=" * 64)
    return code_sortie


if __name__ == "__main__":
    sys.exit(main())

# --------------------------------------------------------------------------
# PLANIFICATION (a documenter dans le livrable E5-P2)
#
# Linux / macOS - crontab -e :
#   30 5 * * *  cd /chemin/tp/code && /usr/bin/python3 07_tp5_pipeline_automatise.py \
#               >> /chemin/tp/logs/cron.log 2>&1
#
# Windows - Planificateur de taches :
#   Programme : python.exe
#   Arguments : 07_tp5_pipeline_automatise.py
#   Demarrer dans : C:\...\tp\code
#   Parametres : "Executer meme si l'utilisateur n'est pas connecte",
#                "Arreter la tache si elle depasse 1 heure"
#
# GitHub Actions (execution planifiee gratuite sur depot public) :
#   on:
#     schedule:
#       - cron: '30 5 * * *'
#
# EXERCICES
# Ils sont enonces dans l'application, fiche « TP 5 - Automatisation de bout
# en bout », section « Exercices » : la panne volontaire, l'archivage et la
# retention des journaux, la notification reelle, et la configuration
# externalisee. C'est la aussi que se rend le travail.
# --------------------------------------------------------------------------
