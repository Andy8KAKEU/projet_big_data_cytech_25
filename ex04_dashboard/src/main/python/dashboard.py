"""Dashboard Streamlit pour la visualisation des données NYC Yellow Taxi.

Ce module implémente un tableau de bord interactif permettant de visualiser
les indicateurs clés de performance (KPI) des courses de taxi jaune de NYC.
Les données sont extraites d'un data warehouse PostgreSQL.

Auteur: CyTech Big Data Project
Date: 2025
"""

import streamlit as st
import pandas as pd
import psycopg2
from psycopg2 import sql

# Configuration de la connexion PostgreSQL
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "nyc_taxi_dw",
    "user": "psg",
    "password": "psg123"
}

# Fonction pour établir une connexion à la base de données
@st.cache_resource
def get_connection():
    """Etablit et retourne une connexion à la base de données PostgreSQL.
    
    La connexion est mise en cache par Streamlit pour éviter les reconnexions multiples.
    
    Returns:
        psycopg2.connection: Objet de connexion PostgreSQL ou None en cas d'erreur
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        st.error(f"Erreur de connexion à la base de données: {e}")
        return None

# Fonction pour exécuter une requête SQL et retourner un DataFrame
def query_to_dataframe(query):
    """Exécute une requête SQL et retourne les résultats dans un DataFrame pandas.
    
    Args:
        query (str): Requête SQL à exécuter
        
    Returns:
        pandas.DataFrame: Résultats de la requête ou None en cas d'erreur
    """
    conn = get_connection()
    if conn:
        try:
            df = pd.read_sql_query(query, conn)
            return df
        except Exception as e:
            st.error(f"Erreur lors de l'exécution de la requête: {e}")
            return None
    return None

# Interface principale Streamlit
st.title("NYC YELLOW TAXI Dashboard")
st.write("Ce tableau de bord présente les indicateurs clés de performance des courses de taxi jaune de NYC.")

# Vérification de la connexion à la base de données
conn = get_connection()
if conn:
    st.success("Connecté à la base de données PostgreSQL")
    
    # KPI 1: Revenue total
    revenue_query = "SELECT SUM(total_amount) as total_revenue FROM fact_trips;"
    revenue_df = query_to_dataframe(revenue_query)
    
    # KPI 2: Total trips
    trips_query = "SELECT COUNT(*) as total_trips FROM fact_trips;"
    trips_df = query_to_dataframe(trips_query)
    
    # KPI 3: Avg revenue per trip
    avg_revenue_query = "SELECT AVG(total_amount) as avg_revenue_per_trip FROM fact_trips;"
    avg_revenue_df = query_to_dataframe(avg_revenue_query)
    
    # KPI 4: Avg distance
    avg_distance_query = "SELECT AVG(trip_distance) as avg_distance FROM fact_trips;"
    avg_distance_df = query_to_dataframe(avg_distance_query)
    
    # KPI 5: Avg duration
    avg_duration_query = "SELECT AVG(EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime))/60) as avg_duration FROM fact_trips;"
    avg_duration_df = query_to_dataframe(avg_duration_query)

    # KPI 5: Durée moyenne par heure de la journée
    duration_by_hour_query = """
                SELECT 
                    EXTRACT(HOUR FROM pickup_datetime) as hour,
                    AVG(EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime))/60) as avg_duration
                FROM fact_trips
                WHERE dropoff_datetime > pickup_datetime
                GROUP BY EXTRACT(HOUR FROM pickup_datetime)
                ORDER BY hour;
            """
    duration_by_hour_df = query_to_dataframe(duration_by_hour_query)
    
    # KPI 6: Pourboire moyen
    avg_tip_query = "SELECT AVG(tip_amount) as avg_tip FROM fact_trips;"
    avg_tip_df = query_to_dataframe(avg_tip_query)

    # Affichage des indicateurs clés de performance (KPI)
    if revenue_df is not None and not revenue_df.empty:
        st.metric("Revenu Total", f"${revenue_df['total_revenue'][0]:,.0f}")
    
    if trips_df is not None and not trips_df.empty:
        st.metric("Nombre Total de Courses", f"{trips_df['total_trips'][0]:,}")
    
    if avg_revenue_df is not None and not avg_revenue_df.empty:
        st.metric("Revenu Moyen par Course", f"${avg_revenue_df['avg_revenue_per_trip'][0]:.0f}")
    
    if avg_distance_df is not None and not avg_distance_df.empty:
        st.metric("Distance Moyenne", f"{avg_distance_df['avg_distance'][0]:.0f} mi")
    
    if avg_duration_df is not None and not avg_duration_df.empty:
        st.metric("Durée Moyenne", f"{avg_duration_df['avg_duration'][0]:.0f} min")
                   
    if duration_by_hour_df is not None and not duration_by_hour_df.empty:
        duration_by_hour_df['hour'] = duration_by_hour_df['hour'].astype(int)
        st.bar_chart(duration_by_hour_df.set_index('hour')['avg_duration'])
        st.markdown("""
                **Lecture du graphique:**
                - **Axe X** : Heures (0 → 23)
                - **Axe Y** : Durée moyenne (minutes)
                - **Pics le matin (11h-12h)** → Heure de pointe travail
                - **Pics le soir (15h-16h)** → Retour domicile
                - **Creux la nuit** → Circulation fluide
                """)
    # KPI 7: Payment distribution
    st.subheader("Payment Distribution")
    payment_query = """
        SELECT 
            p.payment_description,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fact_trips), 2) as percentage
        FROM fact_trips f
        JOIN dim_payment_type p ON f.payment_type_id = p.payment_type_id
        GROUP BY p.payment_description
        ORDER BY count DESC;
    """
    payment_df = query_to_dataframe(payment_query)
    
    if payment_df is not None and not payment_df.empty:
        st.dataframe(payment_df, use_container_width=True)
        
        # Graphique en barres pour la distribution des paiements
        st.bar_chart(payment_df.set_index('payment_description')['count'])
    
    # KPI 8: Trips by hour
    st.subheader("Trips by Hour")
    trips_by_hour_query = """
        SELECT 
            EXTRACT(HOUR FROM pickup_datetime) as hour,
            COUNT(*) as trip_count
        FROM fact_trips
        GROUP BY EXTRACT(HOUR FROM pickup_datetime)
        ORDER BY hour;
    """
    trips_by_hour_df = query_to_dataframe(trips_by_hour_query)
    
    if trips_by_hour_df is not None and not trips_by_hour_df.empty:
        trips_by_hour_df['hour'] = trips_by_hour_df['hour'].astype(int)
        st.line_chart(trips_by_hour_df.set_index('hour')['trip_count'])
    
