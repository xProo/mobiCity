#!/usr/bin/env python3
"""
Fil rouge du module Big Data (M2) - Generateur de donnees "MobiCity".

Contexte : MobiCity est un operateur de mobilite partagee (velos et trottinettes
en libre-service) present dans 6 villes francaises. La direction veut un socle
de donnees permettant de piloter l'activite et d'automatiser le reporting.

Le generateur produit 6 sources HETEROGENES, volontairement imparfaites :

  data/raw/trajets/*.csv          -> structure, gros volume  (transactions)
  data/raw/telemetrie/*.jsonl     -> semi-structure, tres gros volume (IoT)
  data/raw/stations.csv           -> referentiel structure
  data/raw/clients.csv            -> donnees a caractere personnel (RGPD)
  data/raw/maintenance/*.json     -> semi-structure a schema variable
  data/raw/meteo.json             -> source externe type API

Anomalies injectees volontairement (utilisees en TP qualite) :
  - doublons stricts et quasi-doublons
  - formats de date heterogenes (ISO, FR, epoch)
  - villes mal normalisees ("Paris", "paris", "PARIS ", "Pa ris")
  - durees negatives ou aberrantes, distances nulles
  - niveaux de batterie hors bornes (>100, <0)
  - cles orphelines (station_id ou client_id inexistants)
  - valeurs manquantes

Usage :
    python 00_generate_data.py --scale 1        # ~50 000 trajets  (TP standard)
    python 00_generate_data.py --scale 10       # ~500 000 trajets (demo volume)
    python 00_generate_data.py --scale 0.05     # jeu miniature (tests rapides)
"""

import argparse
import csv
import gzip
import json
import os
import random
from datetime import datetime, timedelta, timezone

VILLES = {
    "Paris": (48.8566, 2.3522),
    "Lyon": (45.7640, 4.8357),
    "Marseille": (43.2965, 5.3698),
    "Lille": (50.6292, 3.0573),
    "Bordeaux": (44.8378, -0.5792),
    "Nantes": (47.2184, -1.5536),
}
# Variantes "sales" utilisees pour polluer une partie des lignes
VARIANTES_VILLE = {
    "Paris": ["Paris", "paris", "PARIS", " Paris ", "Pariss"],
    "Lyon": ["Lyon", "lyon", "LYON", "Lyon "],
    "Marseille": ["Marseille", "marseille", "MARSEILLE", "Marseile"],
    "Lille": ["Lille", "lille", "LILLE"],
    "Bordeaux": ["Bordeaux", "bordeaux", "BORDEAUX"],
    "Nantes": ["Nantes", "nantes", "NANTES", "Nante"],
}
TYPES_ENGIN = ["velo", "velo_elec", "trottinette"]
ABONNEMENTS = ["aucun", "mensuel", "annuel", "etudiant", "entreprise"]
PANNES = ["batterie", "frein", "pneu", "vandalisme", "gps", "moteur"]

RNG = random.Random(42)


def horodatage_sale(dt: datetime) -> str:
    """Renvoie une date dans un format volontairement variable."""
    r = RNG.random()
    if r < 0.75:
        return dt.strftime("%Y-%m-%d %H:%M:%S")          # ISO-like
    if r < 0.90:
        return dt.strftime("%d/%m/%Y %H:%M")             # format FR
    if r < 0.97:
        return str(int(dt.replace(tzinfo=timezone.utc).timestamp()))  # epoch
    return ""                                            # manquant


def ecrire_stations(chemin, nb_par_ville=40):
    stations = []
    sid = 1000
    for ville, (lat, lon) in VILLES.items():
        for _ in range(nb_par_ville):
            sid += 1
            stations.append({
                "station_id": f"ST{sid}",
                "nom": f"{ville} - Station {sid}",
                "ville": ville,
                "latitude": round(lat + RNG.uniform(-0.05, 0.05), 6),
                "longitude": round(lon + RNG.uniform(-0.05, 0.05), 6),
                "capacite": RNG.choice([10, 15, 20, 25, 30]),
                "date_ouverture": (datetime(2021, 1, 1) +
                                   timedelta(days=RNG.randint(0, 1400))).strftime("%Y-%m-%d"),
            })
    with open(chemin, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(stations[0].keys()))
        w.writeheader()
        w.writerows(stations)
    return stations


def ecrire_clients(chemin, nb):
    prenoms = ["Camille", "Yanis", "Lea", "Noah", "Sofia", "Adam", "Jade", "Rayan",
               "Louise", "Ilyes", "Manon", "Gabriel", "Ines", "Hugo", "Amira"]
    noms = ["Martin", "Bernard", "Dubois", "Robert", "Petit", "Durand", "Moreau",
            "Laurent", "Simon", "Michel", "Garcia", "Roux", "Fournier", "Girard"]
    clients = []
    for i in range(nb):
        cid = f"CL{100000 + i}"
        prenom, nom = RNG.choice(prenoms), RNG.choice(noms)
        clients.append({
            "client_id": cid,
            "nom": nom,
            "prenom": prenom,
            "email": f"{prenom.lower()}.{nom.lower()}{i}@example.com",
            "telephone": f"06{RNG.randint(10000000, 99999999)}",
            "date_naissance": (datetime(1960, 1, 1) +
                               timedelta(days=RNG.randint(0, 16000))).strftime("%Y-%m-%d"),
            "ville_residence": RNG.choice(list(VILLES.keys())),
            "abonnement": RNG.choices(ABONNEMENTS, weights=[40, 25, 15, 15, 5])[0],
            "date_inscription": (datetime(2021, 1, 1) +
                                 timedelta(days=RNG.randint(0, 1500))).strftime("%Y-%m-%d"),
            "consentement_marketing": RNG.choice(["oui", "non", ""]),
        })
    with open(chemin, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(clients[0].keys()))
        w.writeheader()
        w.writerows(clients)
    return clients


def ecrire_trajets(dossier, stations, clients, nb, meteo_index=None, nb_fichiers=6):
    """
    Ecrit les trajets en plusieurs fichiers CSV (simule une collecte quotidienne).

    Deux signaux "metier" sont volontairement injectes pour que les etudiants
    aient quelque chose a DECOUVRIR dans les donnees :
      - effet meteo : la pluie et le froid font chuter le nombre de trajets
      - poles de flux : certaines stations (les 6 premieres de chaque ville,
        assimilees aux gares/centres d'affaires) recoivent plus d'arrivees le
        matin et plus de departs le soir -> desequilibre a detecter.
    """
    os.makedirs(dossier, exist_ok=True)
    par_ville, poles = {}, {}
    for s in stations:
        par_ville.setdefault(s["ville"], []).append(s["station_id"])
    for ville, liste in par_ville.items():
        poles[ville] = liste[:6]
    debut = datetime(2026, 1, 1)
    colonnes = ["trajet_id", "client_id", "engin_id", "type_engin", "ville",
                "station_depart", "station_arrivee", "debut", "fin",
                "duree_min", "distance_km", "montant_eur", "moyen_paiement"]

    lignes_par_fichier = max(1, nb // nb_fichiers)
    compteur, fichier_idx, f, w = 0, 0, None, None
    doublons = []

    for i in range(nb):
        if compteur % lignes_par_fichier == 0:
            if f:
                f.close()
            fichier_idx += 1
            f = open(os.path.join(dossier, f"trajets_2026_{fichier_idx:02d}.csv"),
                     "w", newline="", encoding="utf-8")
            w = csv.DictWriter(f, fieldnames=colonnes)
            w.writeheader()

        ville = RNG.choices(list(VILLES.keys()), weights=[35, 18, 15, 12, 10, 10])[0]

        # Tirage de la date avec effet meteo : on retire jusqu'a 6 fois une date
        # tant que la meteo du jour "decourage" le trajet.
        for _ in range(6):
            d = debut + timedelta(minutes=RNG.randint(0, 210 * 24 * 60))
            if not meteo_index:
                break
            m = meteo_index.get((ville, d.strftime("%Y-%m-%d")))
            if not m:
                break
            p_abandon = min(0.75, max(0.0, m["pluie_mm"] - 0.5) * 0.14)
            if m["temp_c"] < 5:
                p_abandon += 0.25
            elif m["temp_c"] > 30:
                p_abandon += 0.12
            if RNG.random() > p_abandon:
                break

        # Saisonnalite : plus de trajets aux heures de pointe
        if RNG.random() < 0.55:
            d = d.replace(hour=RNG.choice([7, 8, 9, 17, 18, 19]))
        duree = max(1, int(RNG.lognormvariate(2.5, 0.6)))
        distance = round(duree * RNG.uniform(0.15, 0.35), 2)
        type_engin = RNG.choices(TYPES_ENGIN, weights=[30, 45, 25])[0]
        tarif = {"velo": 0.15, "velo_elec": 0.25, "trottinette": 0.22}[type_engin]
        montant = round(1.0 + duree * tarif, 2)

        # Poles de flux : le matin on va vers les poles, le soir on en repart.
        depart, arrivee = RNG.choice(par_ville[ville]), RNG.choice(par_ville[ville])
        if d.hour in (7, 8, 9) and RNG.random() < 0.45:
            arrivee = RNG.choice(poles[ville])
        elif d.hour in (17, 18, 19) and RNG.random() < 0.45:
            depart = RNG.choice(poles[ville])

        ligne = {
            "trajet_id": f"TR{i:09d}",
            "client_id": RNG.choice(clients)["client_id"],
            "engin_id": f"EN{RNG.randint(1, 8000):05d}",
            "type_engin": type_engin,
            "ville": RNG.choice(VARIANTES_VILLE[ville]) if RNG.random() < 0.12 else ville,
            "station_depart": depart,
            "station_arrivee": arrivee,
            "debut": horodatage_sale(d),
            "fin": horodatage_sale(d + timedelta(minutes=duree)),
            "duree_min": duree,
            "distance_km": distance,
            "montant_eur": montant,
            "moyen_paiement": RNG.choices(["cb", "paypal", "abonnement", ""],
                                          weights=[55, 15, 28, 2])[0],
        }

        # --- injection d'anomalies -------------------------------------
        r = RNG.random()
        if r < 0.010:
            ligne["duree_min"] = -RNG.randint(1, 30)            # duree negative
        elif r < 0.016:
            ligne["duree_min"] = RNG.randint(2000, 9000)        # duree aberrante
        elif r < 0.024:
            ligne["distance_km"] = 0.0                          # distance nulle
        elif r < 0.032:
            ligne["montant_eur"] = ""                           # montant manquant
        elif r < 0.038:
            ligne["client_id"] = f"CL{RNG.randint(900000, 999999)}"  # cle orpheline
        elif r < 0.044:
            ligne["station_arrivee"] = f"ST{RNG.randint(9000, 9999)}"  # cle orpheline

        w.writerow(ligne)
        if RNG.random() < 0.012:          # doublon strict a reinjecter plus tard
            doublons.append(dict(ligne))
        compteur += 1

    # Reinjection des doublons dans un fichier "de rattrapage"
    if f:
        f.close()
    with open(os.path.join(dossier, "trajets_2026_rattrapage.csv"),
              "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=colonnes)
        w.writeheader()
        for ligne in doublons:
            w.writerow(ligne)
    return nb + len(doublons)


def ecrire_telemetrie(dossier, stations, nb_evenements, compresser=True):
    """Flux IoT : une mesure par engin toutes les ~30 s. JSON Lines (gzip)."""
    os.makedirs(dossier, exist_ok=True)
    debut = datetime(2026, 1, 1)
    nb_fichiers = 8
    par_fichier = max(1, nb_evenements // nb_fichiers)
    ecrits = 0
    for idx in range(nb_fichiers):
        nom = os.path.join(dossier, f"telemetrie_part-{idx:02d}.jsonl")
        if compresser:
            nom += ".gz"
            fh = gzip.open(nom, "wt", encoding="utf-8")
        else:
            fh = open(nom, "w", encoding="utf-8")
        with fh as f:
            for _ in range(par_fichier):
                s = RNG.choice(stations)
                ts = debut + timedelta(seconds=RNG.randint(0, 210 * 24 * 3600))
                batterie = RNG.randint(5, 100)
                if RNG.random() < 0.008:
                    batterie = RNG.choice([-5, 120, 999])       # hors bornes
                evt = {
                    "engin_id": f"EN{RNG.randint(1, 8000):05d}",
                    "ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "lat": round(s["latitude"] + RNG.uniform(-0.01, 0.01), 6),
                    "lon": round(s["longitude"] + RNG.uniform(-0.01, 0.01), 6),
                    "batterie_pct": batterie,
                    "vitesse_kmh": round(max(0, RNG.gauss(14, 6)), 1),
                    "statut": RNG.choices(["en_course", "disponible", "hors_service"],
                                          weights=[35, 60, 5])[0],
                    "ville": s["ville"],
                }
                if RNG.random() < 0.05:            # champ optionnel (schema variable)
                    evt["firmware"] = RNG.choice(["2.1.0", "2.2.3", "3.0.0-beta"])
                if RNG.random() < 0.02:            # champ manquant
                    evt.pop("vitesse_kmh", None)
                f.write(json.dumps(evt) + "\n")
                ecrits += 1
    return ecrits


def ecrire_maintenance(dossier, nb):
    """Tickets de maintenance : schema variable, texte libre (non structure)."""
    os.makedirs(dossier, exist_ok=True)
    commentaires = [
        "Frein arriere qui grince, intervention rapide necessaire.",
        "Batterie ne tient plus la charge apres 2 km.",
        "Engin retrouve couche sur la chaussee, guidon tordu.",
        "GPS envoie des positions incoherentes depuis hier.",
        "RAS apres controle visuel.",
        "Pneu avant creve, remplace sur place.",
        "Moteur ne demarre pas, retour atelier.",
    ]
    tickets = []
    for i in range(nb):
        t = {
            "ticket_id": f"MT{i:06d}",
            "engin_id": f"EN{RNG.randint(1, 8000):05d}",
            "date_ouverture": (datetime(2026, 1, 1) +
                               timedelta(days=RNG.randint(0, 210))).strftime("%Y-%m-%d"),
            "type_panne": RNG.choice(PANNES),
            "commentaire": RNG.choice(commentaires),
            "priorite": RNG.choice(["P1", "P2", "P3"]),
        }
        if RNG.random() < 0.6:
            t["date_cloture"] = (datetime.strptime(t["date_ouverture"], "%Y-%m-%d") +
                                 timedelta(days=RNG.randint(0, 20))).strftime("%Y-%m-%d")
        if RNG.random() < 0.3:
            t["cout_eur"] = round(RNG.uniform(5, 350), 2)
        if RNG.random() < 0.15:
            t["technicien"] = {"id": f"TEC{RNG.randint(1, 40):03d}",
                               "equipe": RNG.choice(["nord", "sud", "mobile"])}
        tickets.append(t)

    lot = max(1, nb // 4)
    for idx in range(0, nb, lot):
        with open(os.path.join(dossier, f"maintenance_{idx // lot:02d}.json"),
                  "w", encoding="utf-8") as f:
            json.dump(tickets[idx:idx + lot], f, ensure_ascii=False, indent=1)
    return nb


def ecrire_meteo(chemin):
    """
    Source externe (type reponse d'API) : meteo quotidienne par ville.
    La temperature suit une saisonnalite (janvier froid -> juillet chaud) pour
    que les etudiants retrouvent un signal exploitable dans les analyses.
    """
    import math
    jours = [(datetime(2026, 1, 1) + timedelta(days=d)) for d in range(211)]
    # Temperature moyenne annuelle par ville (approximation)
    moyennes = {"Paris": 12.5, "Lyon": 13.0, "Marseille": 16.0,
                "Lille": 11.0, "Bordeaux": 14.0, "Nantes": 13.0}
    releves, index = [], {}
    for ville in VILLES:
        for j in jours:
            n_jour = j.timetuple().tm_yday
            saison = 9.5 * math.sin(2 * math.pi * (n_jour - 105) / 365)
            temp = round(moyennes[ville] + saison + RNG.gauss(0, 3), 1)
            pluie = round(max(0.0, RNG.gauss(2.2 if n_jour < 120 else 1.2, 3)), 1)
            r = {"ville": ville, "date": j.strftime("%Y-%m-%d"),
                 "temp_c": temp, "pluie_mm": pluie,
                 "vent_kmh": round(max(0, RNG.gauss(15, 8)), 1)}
            releves.append(r)
            index[(ville, r["date"])] = r
    with open(chemin, "w", encoding="utf-8") as f:
        json.dump({"source": "api-meteo-simulee", "unite_temp": "celsius",
                   "releves": releves}, f, ensure_ascii=False)
    return releves, index


def main():
    p = argparse.ArgumentParser(description="Generateur du jeu de donnees MobiCity")
    p.add_argument("--scale", type=float, default=1.0,
                   help="1 = ~50k trajets et ~400k evenements IoT")
    p.add_argument("--out", default="../data", help="dossier de sortie")
    p.add_argument("--no-gzip", action="store_true", help="telemetrie non compressee")
    args = p.parse_args()

    base = os.path.abspath(args.out)
    raw = os.path.join(base, "raw")
    os.makedirs(raw, exist_ok=True)

    nb_trajets = int(50_000 * args.scale)
    nb_clients = max(50, int(12_000 * args.scale))
    nb_telemetrie = int(400_000 * args.scale)
    nb_tickets = max(20, int(3_000 * args.scale))

    print(f"Generation dans {raw} (scale={args.scale})")
    releves, meteo_index = ecrire_meteo(os.path.join(raw, "meteo.json"))
    print(f"  meteo.json            : {len(releves):>9,} releves")
    stations = ecrire_stations(os.path.join(raw, "stations.csv"))
    print(f"  stations.csv          : {len(stations):>9,} lignes")
    clients = ecrire_clients(os.path.join(raw, "clients.csv"), nb_clients)
    print(f"  clients.csv           : {len(clients):>9,} lignes")
    n = ecrire_trajets(os.path.join(raw, "trajets"), stations, clients, nb_trajets,
                       meteo_index=meteo_index)
    print(f"  trajets/*.csv         : {n:>9,} lignes (doublons inclus)")
    n = ecrire_telemetrie(os.path.join(raw, "telemetrie"), stations,
                          nb_telemetrie, compresser=not args.no_gzip)
    print(f"  telemetrie/*.jsonl.gz : {n:>9,} evenements")
    n = ecrire_maintenance(os.path.join(raw, "maintenance"), nb_tickets)
    print(f"  maintenance/*.json    : {n:>9,} tickets")
    print("Termine.")


if __name__ == "__main__":
    main()
