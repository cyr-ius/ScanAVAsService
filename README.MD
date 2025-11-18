# ScanAsService

Permet de réaliser des scan Antivirus des fichiers en mode service.
Le moteur ClamAv sert l'antivirus.
Le moteur Kafka permet d'émettre des messages pour procéder au scan et récupérer le résultat
Le moteur Minio permet de téléversement vers un bucket S3 afin que ClamAv scanne le fichier
 Il est possiblle de récupérer le fichier scanné via le bucket "processed"

## Installation
```bash
git clone http//github.com/cyr-ius/ScanAsService
```

### Lancement de l'application

Construisez et démarrez les conteneurs Docker :

```bash
docker-compose up --build -d
```

## Utilisation

Une fois l'application lancée, vous pouvez déposer un fichier dans le bucket S3 nommé : scans

Emettre un message à Kafka pour réaliser le scan

    -> {"id":"xx","bucket":"files","key":"nom_du_fichier"}


Résultat:

    -> {'id': 'xx', 'bucket': 'files', 'key': 'processed/nom_du_fichier', 'status': 'CLEAN', 'virus': None, 'details': '', 'timestamp': '2025-11-18T12:09:18.079338+00:00'}


Le fichier scanné est téléchargeable sur le bucket "processed"


## Structure du projet

```
.
├── demo/                     # Exécuter le fichier python permet déposer un fichier , de lancer la commande Kafka et récupérer le résultat
├── scanner/                  # Code source du scanner
├── docker-compose.yml        # Configuration Docker Compose
├── README.MD                 # Ce fichier
└── ...
```

## Dépendances

- Kakfa , en écoute sur le port *0.0.0.0:9092*

- ClamAV , en écoute sur le port *0.0.0.0:3310*

- Minio , en écoute sur le port *0.0.0.0:9000

- Scanner est un wrapper python qui effectue une boucle qui permet d'intercepter le message Kafka et de soumettre le fichier à ClamAV. Une fois terminée , cette boucle émet un résultat sur Kafka. Une boucle de nettoyage du bucket "quarantine" est réalisé.


## Contribuer

Nous accueillons les contributions ! Veuillez lire `CONTRIBUTING.md` pour plus de détails sur la façon de contribuer à ce projet.

## Licence

Ce projet est sous licence MIT. Voir le fichier `LICENSE` pour plus de détails.
