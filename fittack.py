from pymongo import MongoClient
from datetime import datetime, timedelta
client=MongoClient()
# print (client)
client=MongoClient('mongodb://localhost:27017/')

db=client['FitTrack']

collection1=db['Utilisateurs']
collection2=db['Activites']
collection3=db['Sessions']

# collection1.insert_many([
#     { "nom": "Amine El Haddad", "age": 29, "sexe": "Homme", "pays": "Maroc"},
#     { "nom": "Sarah Bensaid", "age": 25, "sexe": "Femme", "pays": "France"},
#     { "nom": "Yassine Lamrani", "age": 32, "sexe": "Homme", "pays": "Maroc"},
#     { "nom": "Lina Moreau", "age": 28, "sexe": "Femme", "pays": "Canada"},
#     { "nom": "Karim Ziani", "age": 35, "sexe": "Homme", "pays": "Algerie"}
#   ])
# collection2.insert_many([
#     { "nom": "Course"},
#     { "nom": "Natation"},
#     { "nom": "Cyclisme"},
#     { "nom": "Musculation"},
#     { "nom": "Yoga"}
#   ])

# collection3.insert_many([
#     { "utilisateur_id": 1, "activité_id": 1, "duree": 45, "distance": 10, "calories": 500, "date": "2024-02-10"},
#     { "utilisateur_id": 1, "activité_id": 3, "duree": 60, "distance": 20, "calories": 600, "date": "2024-02-12"},
#     { "utilisateur_id": 2, "activité_id": 2, "duree": 30, "distance": 1, "calories": 300, "date": "2024-02-14"},
#     { "utilisateur_id": 3, "activité_id": 4, "duree": 40, "distance": 0, "calories": 400, "date": "2024-02-15"},
#     { "utilisateur_id": 4, "activité_id": 1, "duree": 50, "distance": 12, "calories": 550, "date": "2024-02-16"},
#     { "utilisateur_id": 5, "activité_id": 5, "duree": 30, "distance": 0, "calories": 200, "date": "2024-02-18"},
#     { "utilisateur_id": 1, "activité_id": 1, "duree": 55, "distance": 11, "calories": 520, "date": "2024-02-20"},
#     { "utilisateur_id": 3, "activité_id": 3, "duree": 90, "distance": 35, "calories": 750, "date": "2024-02-22"},
#     { "utilisateur_id": 2, "activité_id": 2, "duree": 45, "distance": 2, "calories": 450, "date": "2024-02-24"},
#     { "utilisateur_id": 4, "activité_id": 4, "duree": 60, "distance": 0, "calories": 600, "date": "2024-02-25"}
#   ])

# 2.	Listez les utilisateurs ayant effectué au moins 5 sessions de sport au cours du dernier mois.
from datetime import datetime, timedelta

dernier_mois = datetime.today() - timedelta(days=30)

pipeline = [
    {
        "$lookup": {
            "from": "Sessions",  # Jointure avec la collection Sessions
            "localField": "_id",
            "foreignField": "utilisateur_id",
            "as": "sessions"
        }
    },
    {
        "$addFields": {
            "sessions_recentes": {
                "$filter": {
                    "input": "$sessions",
                    "as": "session",
                    "cond": {"$gte": ["$$session.date", dernier_mois]}
                }
            }
        }
    },
    {
        "$addFields": {
            "nbr_sessions": {"$size": "$sessions_recentes"}  # Nombre de sessions récentes
        }
    },
    {
        "$match": {
            "nbr_sessions": {"$gte": 5}  # Filtrer les utilisateurs ayant au moins 5 sessions
        }
    },
    {
        "$project": {
            "_id": 0,
            "nom": 1,
            "nbr_sessions": 1  # Afficher uniquement nom et nombre de sessions
        }
    }
]

resultat = collection1.aggregate(pipeline)

for r in resultat:
    print(f"Les utilisateurs sont : {r['nom']} avec {r['nbr_sessions']} sessions")


# 3.	Quelle est la distance totale parcourue par chaque utilisateur pour chaque type d'activité ?
pipeline1 = [
    {
        "$lookup": {
            "from": "Sessions",  # Jointure avec Sessions
            "localField": "_id",
            "foreignField": "utilisateur_id",
            "as": "sessions"
        }
    },
    {
        "$unwind": "$sessions"  # Désimbriquer la liste sessions
    },
    {
        "$lookup": {
            "from": "Activites",  # Jointure avec Activites
            "localField": "sessions.activité_id",
            "foreignField": "_id",
            "as": "activite"
        }
    },
    {
        "$unwind": "$activite"  # Désimbriquer la liste activite
    },
    {
        "$group": {
            "_id": {
                "utilisateur": "$nom",
                "activite": "$activite.nom"
            },
            "distance_totale": {"$sum": "$sessions.distance"}  # Somme des distances parcourues
        }
    },
    {
        "$project": {
            "_id": 0,
            "utilisateur": "$_id.utilisateur",
            "activite": "$_id.activite",
            "distance_totale": 1
        }
    }
]

resultat = collection1.aggregate(pipeline1)

for r in resultat:
    print(f"Utilisateur: {r['utilisateur']} - Activité: {r['activite']} - Distance totale: {r['distance_totale']} km")




# 4.	Quelles sont les 3 sessions ayant brûlé le plus de calories ?
Session=collection3.sort("calories":-1).limit(3)

