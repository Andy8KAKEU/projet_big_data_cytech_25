import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, lit, when}
import java.sql.DriverManager
import org.apache.spark.sql.functions._
import common.SparkConfig
import common.MinIOClient
import java.util.Properties

/** Module principal du pipeline ETL pour l'ingestion des données NYC Taxi.
  *
  * Ce module implémente le processus ETL complet:
  *   - Extract: Lecture des données brutes depuis MinIO
  *   - Transform: Nettoyage et transformation Bronze vers Silver
  *   - Load: Chargement dans PostgreSQL selon un schéma en étoile
  */
object Main {

  /** Transforme les données brutes (Bronze) en données nettoyées et
    * standardisées (Silver).
    *
    * Cette méthode applique les règles de qualité des données et standardise
    * les noms de colonnes pour correspondre au schéma cible du data warehouse.
    *
    * Règles de nettoyage appliquées:
    *   - Suppression des enregistrements avec timestamps null
    *   - Validation du nombre de passagers (1-9)
    *   - Validation des montants et distances (valeurs positives)
    *   - Règle métier: distance nulle implique montant faible
    *
    * @param rawData
    *   Dataset contenant les données brutes au format YelloxTrip
    * @return
    *   DataFrame nettoyé et standardisé prêt pour le chargement
    */
  def bruteToSilver(
      rawData: Dataset[YelloxTrip]
  ): org.apache.spark.sql.DataFrame = {
    import rawData.sparkSession.implicits._

    println("Début du nettoyage et de la transformation des données...")
    val initialCount = rawData.count()
    println(s"Nombre de lignes avant nettoyage : $initialCount")

    // Étape 1: Nettoyage des données - Application des filtres de qualité
    val cleanedData = rawData
      // Filtrage des valeurs nulles sur les champs obligatoires
      .filter(col("tpep_pickup_datetime").isNotNull)
      .filter(col("tpep_dropoff_datetime").isNotNull)
      .filter(col("PULocationID").isNotNull)
      .filter(col("DOLocationID").isNotNull)
      // Validation du nombre de passagers (entre 1 et 9)
      .filter(col("passenger_count") > 0 && col("passenger_count") <= 9)
      // Validation des valeurs numériques positives
      .filter(col("trip_distance") >= 0)
      .filter(col("fare_amount") >= 0)
      .filter(col("total_amount") >= 0)
      // Règle métier: une distance nulle ne peut pas avoir un montant élevé
      .filter(!(col("trip_distance") === 0 && col("fare_amount") > 10))

    val cleanedCount = cleanedData.count()
    val removedCount = initialCount - cleanedCount

    println(s"Nombre de lignes après nettoyage : $cleanedCount")
    println(s"Nombre de lignes supprimées : $removedCount")

    // Étape 2: Transformation - Standardisation des noms de colonnes
    println("Transformation : Renommage des colonnes pour le schéma cible...")
    val transformedData = cleanedData
      .toDF()
      .withColumnRenamed("VendorID", "vendor_id")
      .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
      .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
      .withColumnRenamed("passenger_count", "passenger_count")
      .withColumnRenamed("trip_distance", "trip_distance")
      .withColumnRenamed("RatecodeID", "rate_code_id")
      .withColumnRenamed("store_and_fwd_flag", "store_and_fwd_flag")
      .withColumnRenamed("PULocationID", "pu_location_id")
      .withColumnRenamed("DOLocationID", "do_location_id")
      .withColumnRenamed("payment_type", "payment_type_id")
      .withColumnRenamed("fare_amount", "fare_amount")
      .withColumnRenamed("extra", "extra")
      .withColumnRenamed("mta_tax", "mta_tax")
      .withColumnRenamed("tip_amount", "tip_amount")
      .withColumnRenamed("tolls_amount", "tolls_amount")
      .withColumnRenamed("improvement_surcharge", "improvement_surcharge")
      .withColumnRenamed("total_amount", "total_amount")
      .withColumnRenamed("congestion_surcharge", "congestion_surcharge")
      .withColumnRenamed("Airport_fee", "airport_fee")
      .withColumnRenamed("cbd_congestion_fee", "cbd_congestion_fee")

    println("Transformation terminée avec succès")
    transformedData
  }

  /** Vide toutes les tables du data warehouse.
    *
    * ATTENTION: Cette méthode supprime toutes les données des tables de faits
    * et de dimensions. Elle doit être utilisée avec précaution, uniquement pour
    * réinitialiser le data warehouse.
    *
    * @param jdbcUrl
    *   URL de connexion JDBC à PostgreSQL
    * @param connectionProperties
    *   Propriétés de connexion (utilisateur, mot de passe)
    */
  def truncate_db(jdbcUrl: String, connectionProperties: Properties): Unit = {
    val conn = DriverManager.getConnection(
      jdbcUrl,
      connectionProperties
    )
    // Requête SQL pour vider toutes les tables avec CASCADE pour gérer les contraintes
    val truncateSql =
      "TRUNCATE TABLE fact_trips, dim_vendor, dim_rate_code, dim_payment_type, dim_date, dim_location CASCADE;"

    val stmt = conn.createStatement()
    try {
      stmt.execute(truncateSql)
      println("Toutes les données ont été supprimées avec succès")
    } finally {
      stmt.close()
      conn.close()
    }
  }

  /** Point d'entrée principal du pipeline ETL.
    *
    * Ce programme orchestre les étapes suivantes:
    *   1. Extraction des données brutes depuis MinIO
    *   2. Transformation Bronze vers Silver (nettoyage et standardisation)
    *   3. Export des données nettoyées vers MinIO
    *   4. Chargement dans PostgreSQL (schéma en étoile avec dimensions et
    *      faits)
    *
    * @param args
    *   Arguments de ligne de commande (non utilisés)
    */
  def main(args: Array[String]): Unit = {

    // Configuration Spark partagée pour toutes les opérations
    val sparkConfig = SparkConfig("DataIngestion")
    val jdbcUrl = sparkConfig.jdbcUrl
    val connectionProperties = sparkConfig.connectionProperties
    val spark = sparkConfig.createSession()

    import spark.implicits._

    // Récupération des paramètres de configuration depuis les variables d'environnement
    val bucket_name = sys.env.getOrElse("BUCKET_NAME", "nyc")
    val file = sys.env.getOrElse(
      "FILE_NAME_BUCKET_FIRST_DEPOSIT",
      "yellow_tripdata_2025-08.parquet"
    )
    val cleaned_bucket_name =
      sys.env.getOrElse("CLEANED_BUCKET_NAME", "nyc-cleaned")

    // Phase 1: Extraction des données brutes depuis MinIO
    println("\n" + "=" * 80)
    println("ÉTAPE 1 : EXTRACTION des données brutes depuis MinIO")
    println("=" * 80)
    val dataraw_set = MinIOClient
      .readParquetFromMinIO(spark, bucket_name, file)
      .as[YelloxTrip]

    val rawCount = dataraw_set.count()
    println(s"$rawCount lignes extraites depuis s3a://$bucket_name/$file")
    dataraw_set.select("PULocationID", "DOLocationID").limit(5).show()

    // Phase 2: Transformation Bronze vers Silver
    println("\n" + "=" * 80)
    println(
      "ÉTAPE 2 : TRANSFORMATION Bronze → Silver (nettoyage + standardisation)"
    )
    println("=" * 80)
    val silver_df = bruteToSilver(dataraw_set)

    // Phase 3: Export des données nettoyées vers MinIO
    println("\n" + "=" * 80)
    println("ÉTAPE 3 : EXPORT des données nettoyées vers MinIO")
    println("=" * 80)
    val cleanedFileName = file.replace(".parquet", "_cleaned.parquet")

    MinIOClient.writeParquetToMinIO(
      spark,
      silver_df,
      cleaned_bucket_name,
      cleanedFileName
    )
    println(
      s"Données nettoyées exportées vers s3a://$cleaned_bucket_name/$cleanedFileName"
    )

    // Phase 4: Chargement dans PostgreSQL (schéma en étoile)
    println("\n" + "=" * 80)
    println("ÉTAPE 4 : CHARGEMENT dans PostgreSQL (schéma en étoile)")
    println("=" * 80)

    // Réinitialisation du data warehouse
    truncate_db(jdbcUrl, connectionProperties)

    // Construction et chargement de la dimension Vendor
    println("Chargement de la dimension Vendor...")
    val dimVendorDF = silver_df
      .select("vendor_id")
      .distinct()
      .filter(col("vendor_id").isNotNull)
      .withColumn(
        "vendor_name",
        when(col("vendor_id") === 1, "Creative Mobile Technologies, LLC")
          .when(col("vendor_id") === 2, "Curb Mobility, LLC")
          .when(col("vendor_id") === 6, "Myle Technologies Inc")
          .when(col("vendor_id") === 7, "Helix")
          .otherwise("Unknown")
      )
    dimVendorDF.write
      .mode("append")
      .jdbc(jdbcUrl, "dim_vendor", connectionProperties)

    // Construction et chargement de la dimension Rate Code
    println("Chargement de la dimension Rate Code...")
    val dimRateCodeDF = silver_df
      .select("rate_code_id")
      .distinct()
      .filter(col("rate_code_id").isNotNull)
      .withColumn(
        "rate_description",
        when(col("rate_code_id") === 1, "Standard rate")
          .when(col("rate_code_id") === 2, "JFK")
          .when(col("rate_code_id") === 3, "Newark")
          .when(col("rate_code_id") === 4, "Nassau or Westchester")
          .when(col("rate_code_id") === 5, "Negotiated fare")
          .when(col("rate_code_id") === 6, "Group ride")
          .when(col("rate_code_id") === 99, "Null/unknown")
          .otherwise("Unknown")
      )
    dimRateCodeDF.write
      .mode("append")
      .jdbc(jdbcUrl, "dim_rate_code", connectionProperties)

    // Construction et chargement de la dimension Payment Type
    println("Chargement de la dimension Payment Type...")
    val dimPaymentTypeDF = silver_df
      .select(col("payment_type_id").alias("payment_type_id"))
      .distinct()
      .filter(col("payment_type_id").isNotNull)
      .withColumn(
        "payment_description",
        when(col("payment_type_id") === 0, "Flex Fare trip")
          .when(col("payment_type_id") === 1, "Credit card")
          .when(col("payment_type_id") === 2, "Cash")
          .when(col("payment_type_id") === 3, "No charge")
          .when(col("payment_type_id") === 4, "Dispute")
          .when(col("payment_type_id") === 5, "Unknown")
          .when(col("payment_type_id") === 6, "Voided trip")
          .otherwise("Unknown")
      )
    dimPaymentTypeDF.write
      .mode("append")
      .jdbc(jdbcUrl, "dim_payment_type", connectionProperties)

    // Construction et chargement de la dimension Location
    // Fusion des zones de pickup et dropoff pour obtenir toutes les locations uniques
    println("Chargement de la dimension Location...")
    val puLocationDF = silver_df
      .select(col("pu_location_id").alias("location_id"))
      .distinct()
      .filter(col("location_id").isNotNull)
    val doLocationDF = silver_df
      .select(col("do_location_id").alias("location_id"))
      .distinct()
      .filter(col("location_id").isNotNull)
    val dimLocationDF = puLocationDF.union(doLocationDF).distinct()
    dimLocationDF.write
      .mode("append")
      .jdbc(jdbcUrl, "dim_location", connectionProperties)

    // Construction et chargement de la dimension Date
    // Fusion des dates de pickup et dropoff avec extraction des composantes temporelles
    println("Chargement de la dimension Date...")
    val pickupDateDF = silver_df
      .select(col("pickup_datetime").alias("date_key"))
      .filter(col("pickup_datetime").isNotNull)
    val dropoffDateDF = silver_df
      .select(col("dropoff_datetime").alias("date_key"))
      .filter(col("dropoff_datetime").isNotNull)
    val dimDateDF = pickupDateDF
      .union(dropoffDateDF)
      .distinct()
      .withColumn("hour", hour(col("date_key")))
      .withColumn("day", dayofmonth(col("date_key")))
      .withColumn("month", month(col("date_key")))
      .withColumn("year", year(col("date_key")))
      .withColumn("weekday", date_format(col("date_key"), "EEEE"))
    dimDateDF.write
      .mode("append")
      .jdbc(jdbcUrl, "dim_date", connectionProperties)

    // Chargement de la table de faits
    println("Chargement de la table de faits fact_trips...")
    silver_df.write
      .mode("append")
      .jdbc(sparkConfig.jdbcUrl, "fact_trips", sparkConfig.connectionProperties)

    // Résumé final du pipeline ETL
    println("\n" + "=" * 80)
    println("PIPELINE ETL TERMINÉ AVEC SUCCÈS")
    println("=" * 80)
    println(s"Extraction  : $rawCount lignes depuis s3a://$bucket_name/$file")
    println(
      s"Nettoyage   : Données nettoyées et exportées vers s3a://$cleaned_bucket_name/$cleanedFileName"
    )
    println(
      s"Chargement  : Données transformées et chargées dans PostgreSQL (schéma en étoile)"
    )
    println("=" * 80)
  }
}
