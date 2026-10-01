#!/usr/bin/env python3
"""
TP 4 (Jour 3) - Interface d'analyse des donnees  [modele pour E5 - Partie 1]
============================================================================

Objectif pedagogique (C30) : construire une interface qui repond a des QUESTIONS
DE DECIDEUR, pas une galerie de graphiques.

Architecture retenue (100 % gratuite, aucun compte a creer) :
    couche gold (Parquet)  ->  DuckDB (moteur SQL analytique)  ->  Streamlit (UI)

Pourquoi ne pas brancher Streamlit sur Spark ? Parce que Spark est un moteur de
TRAITEMENT, pas un moteur de SERVICE : demarrer une session Spark a chaque clic
coute des secondes. On sert les agregats gold avec un moteur leger. C'est
exactement le decoupage batch / serving vu en cours.

Lancement :
    pip install streamlit duckdb plotly
    streamlit run 06_tp4_dashboard.py

Chaque bloc porte un commentaire "DECISION :" qui explique quelle decision
metier l'indicateur doit permettre de prendre. C'est ce que le jury attend.
"""

import os

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

GOLD = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "gold"))

# Palette unique pour tout le tableau de bord (lisibilite et coherence visuelle)
COULEURS = ["#4C6EF5", "#F59F00", "#12B886", "#E8590C", "#7950F2", "#495057"]
GABARIT = "plotly_white"

st.set_page_config(page_title="MobiCity - Pilotage", page_icon=None, layout="wide")


# --------------------------------------------------------------------------
# Acces aux donnees : une seule fonction, mise en cache
# --------------------------------------------------------------------------
@st.cache_data(ttl=300)
def q(sql: str) -> pd.DataFrame:
    """Execute une requete DuckDB directement sur les fichiers Parquet gold."""
    con = duckdb.connect()
    for table in os.listdir(GOLD):
        chemin = os.path.join(GOLD, table)
        if os.path.isdir(chemin):
            con.execute(
                f"CREATE OR REPLACE VIEW {table} AS "
                f"SELECT * FROM read_parquet('{chemin}/**/*.parquet', hive_partitioning=true)")
    try:
        return con.execute(sql).fetchdf()
    finally:
        con.close()


if not os.path.isdir(GOLD):
    st.error("Couche gold introuvable. Executez d'abord les scripts 00 a 04.")
    st.stop()

# --------------------------------------------------------------------------
# En-tete et filtres
# --------------------------------------------------------------------------
st.title("MobiCity - Tableau de bord de pilotage")
st.caption("Source : couche gold du lakehouse | Donnees pseudonymisees (RGPD) | "
           "Les trajets non conformes sont exclus et tracés en quarantaine.")

bornes = q("SELECT min(date) AS d1, max(date) AS d2 FROM kpi_journalier").iloc[0]
villes = q("SELECT DISTINCT ville FROM kpi_journalier ORDER BY 1")["ville"].tolist()

c1, c2 = st.columns([2, 3])
with c1:
    periode = st.date_input("Periode analysee", value=(bornes.d1, bornes.d2),
                            min_value=bornes.d1, max_value=bornes.d2)
with c2:
    selection = st.multiselect("Villes", villes, default=villes)

if len(periode) != 2 or not selection:
    st.warning("Selectionnez une periode complete et au moins une ville.")
    st.stop()

d1, d2 = periode
liste_villes = ", ".join(f"'{v}'" for v in selection)
filtre = f"date BETWEEN DATE '{d1}' AND DATE '{d2}' AND ville IN ({liste_villes})"

# --------------------------------------------------------------------------
# Bandeau d'indicateurs
# DECISION : donner en 5 secondes l'etat de sante commercial de la periode.
# --------------------------------------------------------------------------
kpi = q(f"""
    SELECT sum(nb_trajets)            AS trajets,
           sum(ca_eur)                AS ca,
           sum(clients_actifs)        AS clients,
           avg(panier_moyen_eur)      AS panier,
           avg(duree_moy_min)         AS duree
    FROM kpi_journalier WHERE {filtre}
""").iloc[0]

# Comparaison avec la periode precedente de meme longueur (le chiffre brut ne
# veut rien dire sans point de comparaison).
jours = (d2 - d1).days + 1
precedent = q(f"""
    SELECT sum(nb_trajets) AS trajets, sum(ca_eur) AS ca
    FROM kpi_journalier
    WHERE date BETWEEN DATE '{d1}' - {jours} AND DATE '{d1}' - 1
      AND ville IN ({liste_villes})
""").iloc[0]


def evolution(actuel, ancien):
    if not ancien or pd.isna(ancien) or ancien == 0:
        return None
    return f"{(actuel - ancien) / ancien:+.1%}"


k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Trajets", f"{int(kpi.trajets or 0):,}".replace(",", " "),
          evolution(kpi.trajets, precedent.trajets))
k2.metric("Chiffre d'affaires", f"{kpi.ca or 0:,.0f} EUR".replace(",", " "),
          evolution(kpi.ca, precedent.ca))
k3.metric("Clients actifs (cumul jours)", f"{int(kpi.clients or 0):,}".replace(",", " "))
k4.metric("Panier moyen", f"{kpi.panier or 0:.2f} EUR")
k5.metric("Duree moyenne", f"{kpi.duree or 0:.1f} min")

st.divider()

onglets = st.tabs(["Activite", "Reseau de stations", "Clients", "Flotte", "Qualite des donnees"])

# ------------------------------------------------------------------ Activite
with onglets[0]:
    # DECISION : detecter une rupture de tendance et arbitrer les moyens par ville.
    serie = q(f"""
        SELECT date, ville, sum(nb_trajets) AS trajets, sum(ca_eur) AS ca
        FROM kpi_journalier WHERE {filtre} GROUP BY 1, 2 ORDER BY 1
    """)
    serie["moyenne_7j"] = (serie.groupby("ville")["trajets"]
                           .transform(lambda s: s.rolling(7, min_periods=1).mean()))

    fig = px.line(serie, x="date", y="moyenne_7j", color="ville",
                  title="Trajets quotidiens - moyenne mobile 7 jours",
                  labels={"moyenne_7j": "trajets / jour", "date": ""},
                  color_discrete_sequence=COULEURS, template=GABARIT)
    fig.update_traces(line_width=2)
    st.plotly_chart(fig, width="stretch")
    st.caption("La moyenne mobile lisse l'effet week-end et rend la tendance lisible. "
               "Les creux correspondent aux journees pluvieuses (onglet suivant).")

    g1, g2 = st.columns(2)
    with g1:
        # DECISION : dimensionner la flotte par tranche horaire.
        horaire = q(f"""
            SELECT tranche, type_engin, sum(nb_trajets) AS trajets
            FROM usage_horaire WHERE ville IN ({liste_villes})
            GROUP BY 1, 2
        """)
        ordre = ["matin_pointe", "journee", "soir_pointe", "nuit"]
        fig = px.bar(horaire, x="tranche", y="trajets", color="type_engin",
                     category_orders={"tranche": ordre}, barmode="group",
                     title="Usage par tranche horaire et type d'engin",
                     color_discrete_sequence=COULEURS, template=GABARIT)
        st.plotly_chart(fig, width="stretch")
    with g2:
        # DECISION : anticiper la demande selon la meteo (planning des equipes).
        meteo = q(f"""
            SELECT meteo_categorie,
                   round(avg(nb_trajets), 1) AS trajets_moyens,
                   count(*) AS jours
            FROM frequentation_meteo WHERE {filtre}
            GROUP BY 1 ORDER BY 2 DESC
        """)
        fig = px.bar(meteo, x="meteo_categorie", y="trajets_moyens", text="jours",
                     title="Frequentation moyenne selon la meteo (etiquette = nb de jours)",
                     color_discrete_sequence=COULEURS, template=GABARIT)
        st.plotly_chart(fig, width="stretch")
        st.caption("Lecture : un jour pluvieux fait chuter la frequentation. "
                   "Attention, correlation n'est pas causalite : verifier le nombre "
                   "de jours observes avant de conclure.")

# ----------------------------------------------------------------- Stations
with onglets[1]:
    st.markdown("**Desequilibre du reseau aux heures de pointe du matin**")
    st.caption("Sur l'ensemble de la journee, arrivees et departs s'equilibrent : "
               "l'indicateur global ne montre rien. Le desequilibre n'apparait qu'a "
               "la maille horaire - le choix de la granularite est une decision d'analyse.")

    stations = q(f"""
        SELECT nom, ville, capacite, departs_matin, arrivees_matin,
               flux_net_matin, taux_remplissage_matin, alerte, latitude, longitude
        FROM performance_stations WHERE ville IN ({liste_villes})
    """)
    c1, c2 = st.columns([3, 2])
    with c1:
        fig = px.scatter(stations, x="departs_matin", y="arrivees_matin",
                         size="capacite", color="alerte", hover_name="nom",
                         title="Departs vs arrivees (7h-9h) - la diagonale = equilibre",
                         color_discrete_sequence=COULEURS, template=GABARIT)
        maxi = max(stations.departs_matin.max(), stations.arrivees_matin.max())
        fig.add_shape(type="line", x0=0, y0=0, x1=maxi, y1=maxi,
                      line=dict(dash="dot", color="#adb5bd"))
        st.plotly_chart(fig, width="stretch")
    with c2:
        st.markdown("**Stations a rebalancer en priorite**")
        st.dataframe(
            stations[stations.alerte != "equilibre"]
            .reindex(stations.flux_net_matin.abs().sort_values(ascending=False).index)
            [["nom", "capacite", "flux_net_matin", "alerte"]].head(12),
            hide_index=True, width="stretch")

    st.map(stations.rename(columns={"latitude": "lat", "longitude": "lon"}),
           size="capacite")

# ------------------------------------------------------------------ Clients
with onglets[2]:
    # DECISION : orienter l'offre d'abonnement.
    seg = q("SELECT * FROM segments_clients")
    c1, c2 = st.columns(2)
    with c1:
        fig = px.bar(seg, x="abonnement", y="ca_eur", color="tranche_age",
                     title="Chiffre d'affaires par abonnement et tranche d'age",
                     color_discrete_sequence=COULEURS, template=GABARIT)
        st.plotly_chart(fig, width="stretch")
    with c2:
        fig = px.scatter(seg, x="clients", y="trajets_par_client", color="abonnement",
                         size="ca_eur", hover_name="tranche_age",
                         title="Volume de clients vs intensite d'usage",
                         color_discrete_sequence=COULEURS, template=GABARIT)
        st.plotly_chart(fig, width="stretch")
    st.caption("Aucune donnee directement identifiante n'est utilisee : "
               "l'identifiant client est pseudonymise (SHA-256) et l'age est agrege "
               "en tranches. Principe de minimisation (RGPD, art. 5).")

# ------------------------------------------------------------------- Flotte
with onglets[3]:
    # DECISION : declencher les interventions de maintenance.
    flotte = q("""
        SELECT priorite_intervention, count(*) AS engins,
               round(avg(batterie_moy), 1) AS batterie_moyenne,
               round(avg(nb_tickets), 2) AS tickets_moyens
        FROM sante_flotte GROUP BY 1 ORDER BY 2 DESC
    """)
    c1, c2 = st.columns([2, 3])
    with c1:
        st.dataframe(flotte, hide_index=True, width="stretch")
    with c2:
        urgents = q("""
            SELECT engin_id, batterie_moy, batterie_min, nb_tickets, tickets_p1, score_risque
            FROM sante_flotte WHERE priorite_intervention = 'urgente'
            ORDER BY score_risque DESC LIMIT 20
        """)
        st.markdown("**Engins a retirer du service en priorite**")
        st.dataframe(urgents, hide_index=True, width="stretch")
    st.caption("Le score de risque combine batterie, tickets de maintenance et temps "
               "hors service. Les ponderations doivent etre validees avec l'exploitation : "
               "un indicateur compose n'est credible que si sa formule est documentee.")

# ------------------------------------------------------------ Qualite donnees
with onglets[4]:
    # DECISION : savoir a quel point on peut faire confiance aux chiffres ci-dessus.
    st.markdown("**Ce tableau de bord ne montre que des donnees conformes.**")
    rapport = os.path.join(os.path.dirname(__file__), "..", "logs",
                           "qualite_trajets_silver.json")
    if os.path.exists(rapport):
        import json
        with open(rapport, encoding="utf-8") as f:
            r = json.load(f)
        st.write(f"Dernier controle : {r['horodatage']} | "
                 f"{r['lignes_controlees']:,} lignes | "
                 f"statut : {'CONFORME' if r['conforme'] else 'NON CONFORME'}")
        st.dataframe(pd.DataFrame(r["resultats"]), hide_index=True, width="stretch")
    else:
        st.info("Executez 03_tp2_silver_qualite.py pour generer le rapport qualite.")

    quarantaine = os.path.abspath(os.path.join(GOLD, "..", "quarantaine", "trajets"))
    if os.path.isdir(quarantaine):
        rejets = duckdb.sql(
            f"SELECT motif_rejet, count(*) AS lignes "
            f"FROM read_parquet('{quarantaine}/**/*.parquet', hive_partitioning=true) "
            f"GROUP BY 1 ORDER BY 2 DESC").fetchdf()
        fig = px.bar(rejets, x="lignes", y="motif_rejet", orientation="h",
                     title="Trajets ecartes par motif (quarantaine)",
                     color_discrete_sequence=COULEURS, template=GABARIT)
        st.plotly_chart(fig, width="stretch")
        st.caption("Afficher la quarantaine dans le tableau de bord est un choix de "
                   "transparence : le decideur sait ce qui n'est PAS compte.")

# --------------------------------------------------------------------------
# EXERCICES
# --------------------------------------------------------------------------
# Ils sont enonces dans l'application, fiche « TP 4 - Interface d'analyse »,
# section « Exercices » : le tableau decision / question / indicateur, les
# trois ajouts a cette application (filtre type d'engin, taux de rejet,
# export terrain), le comptage des clients actifs, et la variante propre a
# votre groupe. C'est la aussi que se rend le travail.
