# Big Data M2 — Travaux pratiques (fil rouge MobiCity)

Support de TP du module Big Data (35 h, Bloc 5, RNCP 39765). Tout s'exécute
en local : aucun compte cloud, aucune carte bancaire, aucune dépendance payante.

## 1. Installation

```bash
python -m venv .venv
source .venv/bin/activate        # Windows : .venv\Scripts\activate
pip install -r requirements.txt
```

Prérequis : Python 3.10+ et Java 17 ou 21 (`java -version`).

> **Si l'installation de PySpark échoue** avec une erreur mentionnant
> `install_layout`, c'est un conflit avec setuptools ≥ 81 :
> `pip install "setuptools<81"` puis relancer l'installation.

## 2. Ordre d'exécution

```bash
cd code

python 00_generate_data.py --scale 1        # génère data/raw/  (~2 min)

python 01_tp1_formats_partitionnement.py    # TP1 — J1 — formats, partitionnement
python 02_tp2_bronze_ingestion.py           # TP2a — J2 — ingestion bronze
python 03_tp2_silver_qualite.py             # TP2b — J2 — nettoyage, qualité, RGPD
python 04_tp3_gold_agregats.py              # TP3a — J2 — couche gold
python 05_tp3_optimisation.py               # TP3b — J2 — shuffle, broadcast, cache

streamlit run 06_tp4_dashboard.py           # TP4 — J3 — interface d'analyse
python 07_tp5_pipeline_automatise.py        # TP5 — J3 — chaîne automatisée
```

## 3. Ce que produit chaque script

| Script | Produit | Notion clé |
|---|---|---|
| `00_generate_data.py` | `data/raw/` — 6 sources hétérogènes et sales | Variété, véracité |
| `01_tp1_...` | `data/banc_essai/` — 4 configurations comparées | Formats, partition pruning |
| `02_tp2_bronze...` | `data/bronze/` | Ingestion fidèle, traçabilité |
| `03_tp2_silver...` | `data/silver/`, `data/quarantaine/`, `logs/qualite_*.json` | Qualité, quarantaine, pseudonymisation |
| `04_tp3_gold...` | `data/gold/` — 6 tables d'indicateurs | Agrégation, granularité |
| `05_tp3_optim...` | Mesures en console + plans d'exécution | Shuffle, broadcast, cache |
| `06_tp4_dashboard.py` | Application web locale | Restitution décisionnelle |
| `07_tp5_pipeline...` | `logs/`, `restitution/` | Idempotence, portes qualité, journalisation |

## 4. Options utiles

```bash
python 00_generate_data.py --scale 5           # volume ×5 (essais de montée en charge)
python 00_generate_data.py --scale 0.1         # jeu miniature (démonstration rapide)

python 07_tp5_pipeline_automatise.py --simulation      # dry-run
python 07_tp5_pipeline_automatise.py --depuis gold     # reprise après incident
python 07_tp5_pipeline_automatise.py --forcer          # ignore l'empreinte des sources
python 07_tp5_pipeline_automatise.py --seulement qualite
```

Codes de sortie du pipeline : `0` succès, `1` échec technique, `2` échec qualité bloquant.

## 5. Interface Spark

Pendant l'exécution d'un script PySpark, ouvrir <http://localhost:4040> :
onglets **Jobs**, **Stages**, **SQL** et **Storage**. C'est le meilleur outil
pour faire comprendre le shuffle et la mise en cache.

## 6. Anomalies volontairement injectées

Ne pas les révéler avant le TP 2 — leur découverte est le moment pédagogique
du module.

- trois formats d'horodatage (ISO, français, epoch) et des valeurs vides ;
- doublons stricts réinjectés par un fichier « de rattrapage » ;
- libellés de villes non normalisés (`PARIS`, ` Paris `, `Pariss`) ;
- durées négatives ou aberrantes, distances nulles, montants manquants ;
- niveaux de batterie hors bornes (−5, 120, 999) ;
- clés orphelines (clients et stations inexistants) ;
- schéma variable sur la télémétrie et les tickets de maintenance.

Deux signaux métier réels sont également cachés dans les données, à découvrir
par l'analyse : un effet météo sur la fréquentation, et un déséquilibre des
stations visible uniquement à la maille horaire du matin.

## 7. Arborescence

```
tp/
  code/          scripts numérotés + bibliothèque commune mobicity.py
  data/          raw, bronze, silver, gold, quarantaine   (généré)
  logs/          journaux, rapports qualité, alertes      (généré)
  restitution/   exports automatiques du pipeline         (généré)
```

Seul `code/` est versionné ; les autres répertoires sont régénérables.
