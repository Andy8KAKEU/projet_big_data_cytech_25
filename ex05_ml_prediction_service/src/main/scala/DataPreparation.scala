import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}

/** Objet pour la préparation des données et le feature engineering Source:
  * MinIO Parquet (nyc-cleaned)
  */
object DataPreparation {

  /** Prépare les features pour le modèle de prédiction de pourboire
    *
    * @param data
    *   DataFrame contenant les données brutes du Parquet nettoyé
    * @return
    *   DataFrame avec les features assemblées et la variable cible
    */
  def prepareFeatures(data: DataFrame): DataFrame = {
    // 1. Créer la variable cible (label): 1 si tip_amount > 0, 0 sinon
    val dataWithLabel = data.withColumn(
      "label",
      when(col("tip_amount") > 0, 1.0).otherwise(0.0)
    )

    // 2. Feature engineering: créer des features dérivées
    val enrichedData = dataWithLabel
      // Extraire l'heure de pickup (l'heure influence les pourboires)
      .withColumn("pickup_hour", hour(col("pickup_datetime")).cast("double"))
      // Calculer la durée du trajet en minutes
      .withColumn(
        "trip_duration_minutes",
        (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(
          col("pickup_datetime")
        )) / 60.0
      )

    // 3. Définir les features (8 originales + 6 nouvelles = 14 features)
    val featureColumns = Array(
      "fare_amount", // Montant de la course
      "trip_distance", // Distance du trajet
      "passenger_count", // Nombre de passagers
      "rate_code_id", // Type de tarif
      "payment_type_id", // Type de paiement
      "extra", // Frais supplémentaires
      "mta_tax", // Taxe MTA
      "tolls_amount", // Péages
      "pickup_hour", // Heure de prise en charge (dérivée)
      "trip_duration_minutes", // Durée du trajet en minutes (dérivée)
      "congestion_surcharge", // Surcharge de congestion
      "airport_fee", // Frais d'aéroport
      "cbd_congestion_fee", // Frais de congestion CBD
      "improvement_surcharge" // Surcharge d'amélioration
    )

    // 4. Filtrer les données invalides
    val cleanData = enrichedData
      .filter(col("fare_amount") > 0)
      .filter(col("trip_distance") > 0)
      .filter(col("passenger_count") > 0)
      .filter(
        col("trip_duration_minutes") > 0 && col("trip_duration_minutes") < 300
      )
      .na
      .fill(0.0, featureColumns) // Remplacer les valeurs nulles par 0

    // 5. Assembler les features dans un vecteur
    val assembler = new VectorAssembler()
      .setInputCols(featureColumns)
      .setOutputCol("features_raw")
      .setHandleInvalid("skip")

    val assembledData = assembler.transform(cleanData)

    // 6. Normaliser les features avec StandardScaler
    val scaler = new StandardScaler()
      .setInputCol("features_raw")
      .setOutputCol("features")
      .setWithStd(true)
      .setWithMean(true)

    val scalerModel = scaler.fit(assembledData)
    val scaledData = scalerModel.transform(assembledData)

    // 7. Sélectionner uniquement les colonnes nécessaires
    scaledData.select("features", "label")
  }

  /** Affiche des statistiques sur les données préparées
    */
  def showDataStats(data: DataFrame): Unit = {
    println("\n--- Statistiques des données ---")

    // Distribution de la variable cible
    println("\nDistribution de la variable cible (label):")
    data
      .groupBy("label")
      .count()
      .withColumn("percentage", round(col("count") * 100.0 / data.count(), 2))
      .show()

    // Statistiques descriptives
    println("\nStatistiques descriptives:")
    data.describe().show()
  }
}
