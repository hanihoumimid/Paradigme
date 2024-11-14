import redis
import json

# Connexion au serveur Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Fonction pour charger un fichier JSON et l'importer dans Redis
def import_json_to_redis(filename, redis_key_prefix):
    with open(filename, 'r', encoding='utf-8') as file:
        data = json.load(file)

    # Parcours de chaque élément et insertion dans Redis sous forme de HASH
    for key, value in data['test'].items():
        redis_key = f"{redis_key_prefix}:{key}"  # Exemple de clé: "clients:Rue De Fabre"
        # Utilisation de hset() pour ajouter les champs un par un dans le hachage Redis
        for field, field_value in value.items():
            r.hset(redis_key, field, field_value)  # Insertion sous forme de hachage (HASH)

# Importation des différents fichiers JSON
import_json_to_redis('clients.json', 'clients')
import_json_to_redis('pilots.json', 'pilots')
import_json_to_redis('reservations.json', 'reservations')
import_json_to_redis('vols.json', 'vols')
import_json_to_redis('avions.json', 'avions')
import_json_to_redis('defclasses.json', 'defclasses')

print("Importation des fichiers JSON dans Redis terminée.")

