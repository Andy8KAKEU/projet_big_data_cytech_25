# Projet Big Data - NYC Yellow Taxi Data Pipeline

Ce projet implémente un pipeline complet de traitement de données Big Data pour analyser les données des taxis jaunes de New York City (NYC). Le pipeline comprend la récupération des données, le nettoyage, le stockage dans un data warehouse, la visualisation via un dashboard et des prédictions par machine learning.

## Table des matières

- [Architecture du projet](#architecture-du-projet)
- [Exécution du projet](#exécution-du-projet)
- [Structure du projet](#structure-du-projet)
- [Modalités de rendu](#modalités-de-rendu)

## Architecture du projet

Le projet est composé de 6 exercices interconnectés :

1. **ex01_data_retrieval** : Téléchargement des données NYC Taxi depuis l'API
2. **ex02_data_ingestion** : Nettoyage et transformation des données
3. **ex03_sql_table_creation** : Création du schéma en étoile PostgreSQL
4. **ex04_dashboard** : Calcul des KPIs et dashboard Streamlit
5. **ex05_ml_prediction_service** : Prédictions ML avec Spark MLlib
6. **ex06_airflow** : Orchestration du pipeline avec Apache Airflow

### Technologies utilisées

- **Apache Spark** : Traitement distribué des données
- **MinIO** : Stockage S3-compatible
- **PostgreSQL** : Data warehouse
- **Apache Airflow** : Orchestration
- **Streamlit** : Dashboard de visualisation
- **Docker** : Conteneurisation des services

## Exécution du projet

### Méthode 1 : Exécution automatique avec Airflow (Recommandée)

Cette méthode orchestre automatiquement toutes les étapes du pipeline.

#### Étape 1 : Démarrer les services Docker

```bash
# Démarrer tous les services (Spark, MinIO, PostgreSQL, Airflow)
docker-compose up -d

# Vérifier que tous les services sont démarrés
docker-compose ps
```

Vous devriez voir les services suivants en état "running" :
- `spark-master`
- `spark-worker-1`
- `spark-worker-2`
- `minio`
- `postgres_data_warehouse`
- `postgres_airflow`
- `airflow-webserver`
- `airflow-scheduler`

#### Étape 2 : Accéder à l'interface Airflow

1. Ouvrir un navigateur et aller sur : http://localhost:8082
2. Se connecter avec les identifiants :
   - **Username** : `airflow`
   - **Password** : `airflow`

#### Étape 3 : Exécuter le pipeline

1. Dans l'interface Airflow, trouver le DAG `nyc_taxi_pipeline`
2. Activer le DAG en cliquant sur le toggle (si désactivé)
3. Cliquer sur le bouton "Play" (▶️) pour déclencher une exécution manuelle
4. Suivre la progression dans la vue "Graph" ou "Grid"

Le pipeline exécutera automatiquement les étapes suivantes :
- **ex01_download_data** : Téléchargement des données
- **ex02_data_ingestion** : Nettoyage et transformation
- **ex03_schema_creation** : Chargement dans PostgreSQL
- **ex04_dashboard_kpi** et **ex05_ml_prediction** : Exécution en parallèle

#### Étape 4 : Lancer le dashboard Streamlit

Une fois le pipeline terminé avec succès :

```bash
# Activer l'environnement virtuel si nécessaire
source .venv/bin/activate

# Lancer le dashboard
streamlit run ex04_dashboard/src/main/python/dashboard.py
```

Le dashboard sera accessible sur : http://localhost:8501

### Méthode 2 : Exécution manuelle étape par étape

Si vous préférez exécuter chaque étape manuellement :

#### Étape 1 : Démarrer uniquement les services de base

```bash
# Démarrer MinIO et PostgreSQL uniquement
docker-compose up -d minio postgres-dw
```

#### Étape 2 : Exécuter les exercices avec SBT

```bash
# Ex01 : Téléchargement des données
sbt ex1/run

# Ex02 : Nettoyage et transformation
sbt ex2/run

# Ex03 : Chargement dans PostgreSQL
# Note: Les tables sont créées automatiquement au démarrage de PostgreSQL
# via les scripts SQL dans ex03_sql_table_creation/

# Ex04 : Calcul des KPIs
sbt ex4/run

# Ex05 : Prédictions ML
sbt ex5/run
```

#### Étape 3 : Lancer le dashboard

```bash
streamlit run ex04_dashboard/src/main/python/dashboard.py
```


### Interfaces web

- **Airflow UI** : http://localhost:8082 (Username: `airflow`, Password: `airflow`)
- **MinIO Console** : http://localhost:9001 (Username: `minio`, Password: `minio123`)
- **Dashboard Streamlit** : http://localhost:8501

## Structure du projet

```
projet_big_data_cytech_25/
├── common/                          # Module commun (MinIOClient, SparkConfig)
├── ex01_data_retrieval/            # Téléchargement des données
├── ex02_data_ingestion/            # Nettoyage et transformation
├── ex03_sql_table_creation/        # Scripts SQL pour PostgreSQL
├── ex04_dashboard/                 # Dashboard Streamlit et calcul KPIs
├── ex05_ml_prediction_service/     # Service de prédiction ML
├── ex06_airflow/                   # DAGs Airflow
│   ├── dags/                       # Définition du pipeline
│   ├── logs/                       # Logs d'exécution
│   └── plugins/                    # Plugins personnalisés
├── docker/                         # Dockerfiles personnalisés
├── docker-compose.yml              # Configuration des services
├── build.sbt                       # Configuration SBT
└── README.md                       # Ce fichier
```

## Configuration MinIO pour Spark

Code minimal pour se connecter à MinIO avec Spark :

```scala
import org.apache.spark.sql.{SparkSession, DataFrame}

object SparkApp extends App {
  val spark = SparkSession.builder()
    .appName("SparkApp")
    .master("local")
    .config("fs.s3a.access.key", "minio")
    .config("fs.s3a.secret.key", "minio123")
    .config("fs.s3a.endpoint", "http://localhost:9000/")
    .config("fs.s3a.path.style.access", "true")
    .config("fs.s3a.connection.ssl.enable", "false")
    .config("fs.s3a.attempts.maximum", "1")
    .config("fs.s3a.connection.establish.timeout", "6000")
    .config("fs.s3a.connection.timeout", "5000")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
}
```

## Modalités de rendu

1. Pull Request vers la branch `master`
2. Dépôt du rapport et du code source zippé dans cours.cyu.fr (Les accès seront bientôt ouverts)

**Date limite de rendu** : 7 février 2026