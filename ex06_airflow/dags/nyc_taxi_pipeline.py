"""
NYC Taxi Data Pipeline DAG
Orchestrates the complete end-to-end pipeline for NYC taxi data processing

Pipeline Flow:
1. ex01: Data Retrieval - Download NYC taxi data
2. ex02: Data Ingestion - Clean and transform data
3. ex03: Schema Creation - Load into PostgreSQL star schema
4. ex04 & ex05: Parallel execution - Dashboard KPIs and ML predictions
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# ==========================================
# Configuration du DAG
# ==========================================

default_args = {
    'owner': 'cytech',
    'depends_on_past': False,
    'retries': 2,                          # Nombre de tentatives en cas d'échec
    'retry_delay': timedelta(minutes=3),   # Délai entre chaque tentative
}

# ==========================================
# Définition du DAG
# ==========================================

with DAG(
    dag_id='nyc_taxi_pipeline',
    default_args=default_args,
    description='Pipeline NYC Taxi - Exécution à la demande',
    schedule_interval=None,                # Exécution manuelle uniquement
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['nyc-taxi', 'learning', 'on-demand'],
) as dag:

    # Tâche 1: Téléchargement des données NYC Taxi depuis l'API (ex01)
    task_data_retrieval = BashOperator(
        task_id='ex01_download_data',
        bash_command='''
        set -e  # Arrêter l'exécution en cas d'erreur
        
        echo "[ex01] Téléchargement des données NYC Taxi..."
        cd /opt/airflow/projet_big_data_cytech
        
        if sbt ex1/run; then
            echo "[SUCCES] ex01 terminé avec succès"
        else
            echo "[ERREUR] ex01 a échoué"
            exit 1
        fi
        ''',
        retries=3,  # Nombre de tentatives en cas d'échec
    )

    # ==========================================
    # Tâche 2: ex02 - Data Ingestion
    # ==========================================
    task_data_ingestion = BashOperator(
        task_id='ex02_data_ingestion',
        bash_command='''
        set -e
        
        echo " ex02: Nettoyage et transformation des données..."
        cd /opt/airflow/projet_big_data_cytech
        
        if sbt ex2/run; then
            echo "ex02 terminé avec succès!"
        else
            echo " ERREUR: ex02 a échoué!"
            exit 1
        fi
        ''',
    )

    # Tâche 3: Création du schéma PostgreSQL (ex03)
    task_schema_creation = BashOperator(
        task_id='ex03_schema_creation',
        bash_command='''
        set -e
        
        echo "[ex03] Création du schéma PostgreSQL..."
        cd /opt/airflow/projet_big_data_cytech
        
        if sbt ex3/run; then
            echo "[SUCCES] ex03 terminé avec succès"
        else
            echo "[ERREUR] ex03 a échoué"
            exit 1
        fi
        ''',
    )

    # Tâche 4: Calcul des KPIs pour le dashboard (ex04)
    task_dashboard_kpi = BashOperator(
        task_id='ex04_dashboard_kpi',
        bash_command='''
        set -e
        
        echo "[ex04] Calcul des KPIs pour le dashboard..."
        cd /opt/airflow/projet_big_data_cytech
        
        if sbt ex4/run; then
            echo "[SUCCES] ex04 terminé avec succès"
        else
            echo "[ERREUR] ex04 a échoué"
            exit 1
        fi
        ''',
        retries=1,
    )

    # Tâche 5: Exécution des prédictions ML (ex05)
    task_ml_prediction = BashOperator(
        task_id='ex05_ml_prediction',
        bash_command='''
        set -e
        
        echo "[ex05] Exécution des prédictions ML..."
        cd /opt/airflow/projet_big_data_cytech
        
        if sbt ex5/run; then
            echo "[SUCCES] ex05 terminé avec succès"
        else
            echo "[ERREUR] ex05 a échoué"
            exit 1
        fi
        ''',
    )

    # Définition des dépendances entre les tâches
    # Pipeline linéaire: ex01 -> ex02 -> ex03
    # Puis parallèle: ex03 -> [ex04, ex05]
    task_data_retrieval >> task_data_ingestion >> task_schema_creation >> [task_dashboard_kpi, task_ml_prediction]