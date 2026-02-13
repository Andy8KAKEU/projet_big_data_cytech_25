import common.SparkConfig
import common.MinIOClient

object Main {
  def main(args: Array[String]): Unit = {
    // Initialisation de la session Spark avec MLlib
    val sparkConfig = SparkConfig("TipPredictionML")
    val spark = sparkConfig.createSession()
    import spark.implicits._

    println("=" * 80)
    println("ML PREDICTION SERVICE - TIP CLASSIFICATION")
    println("Source: MinIO Parquet (nyc-cleaned)")
    println("=" * 80)

    try {
      // 1. Chargement des données depuis Minio (Parquet nettoyé)
      println("\n[1/5] Chargement des données depuis MinIO (Parquet)...")
      val data = MinIOClient.readParquetFromMinIO(
        spark,
        sys.env.getOrElse("CLEANED_BUCKET_NAME", "nyc-cleaned"),
        sys.env.getOrElse(
          "FILE_NAME_BUCKET_FIRST_DEPOSIT",
          "yellow_tripdata_2025-08_cleaned.parquet"
        )
      )
      println(s"   ✓ Données chargées: ${data.count()} lignes")

      // Affichage du schéma Parquet
      println("\n   Schéma du dataset Parquet:")
      data.printSchema()
      data.show(5, truncate = false)

      // 2. Préparation des données et feature engineering
      println("\n[2/5] Préparation des données et feature engineering...")
      val preparedData = DataPreparation.prepareFeatures(data)
      println(s"   ✓ Features préparées")

      // Affichage du schéma des features
      println("\n   Schema des features:")
      preparedData.select("features", "label").printSchema()

      // Statistiques sur les données préparées
      DataPreparation.showDataStats(preparedData)

      // 3. Division train/test
      println("\n[3/5] Division des données (train/test)...")
      val Array(trainingData, testData) =
        preparedData.randomSplit(Array(0.8, 0.2), seed = 42)
      println(s"   ✓ Training set: ${trainingData.count()} lignes")
      println(s"   ✓ Test set: ${testData.count()} lignes")

      // 4. Entraînement du modèle
      println("\n[4/5] Entraînement du modèle de classification...")
      val model = TipPredictionModel.trainModel(trainingData)
      println(s"   ✓ Modèle entraîné avec succès")

      // 5. Évaluation du modèle
      println("\n[5/5] Évaluation du modèle...")
      val predictions = model.transform(testData)
      ModelEvaluation.evaluate(predictions)

      println("\n" + "=" * 80)
      println("ENTRAÎNEMENT ET ÉVALUATION TERMINÉS AVEC SUCCÈS")
      println("=" * 80)

    } catch {
      case e: Exception =>
        println(s"\n❌ ERREUR: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

}
