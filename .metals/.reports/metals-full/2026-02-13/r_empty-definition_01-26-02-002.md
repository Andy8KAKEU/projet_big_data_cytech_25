error id: file://<WORKSPACE>/ex05_ml_prediction_service/src/main/scala/Main.scala:`<error>`#`<error>`.
file://<WORKSPACE>/ex05_ml_prediction_service/src/main/scala/Main.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb

found definition using fallback; symbol readParquetFromMinIO
offset: 824
uri: file://<WORKSPACE>/ex05_ml_prediction_service/src/main/scala/Main.scala
text:
```scala
import common.SparkConfig
import  common.MinIOClient
import ml._

object Main {
  def main(args: Array[String]): Unit = {
    // Initialisation de la session Spark avec MLlib
    val sparkConfig = SparkConfig("TipPredictionML")
    val jdbcUrl = sparkConfig.jdbcUrl
    val connectionProperties = sparkConfig.connectionProperties
    val spark = sparkConfig.createSession()
    import spark.implicits._

    println("=" * 80)
    println("ML PREDICTION SERVICE - TIP CLASSIFICATION")
    println("=" * 80)

    try {
      // 1. Chargement des données depuis PostgreSQL
      println("\n[1/5] Chargement des données depuis Minio...")
      //val data = spark.read.jdbc(jdbcUrl, "fact_trips", connectionProperties)
      //println(s"   ✓ Données chargées: ${data.count()} lignes")
      val data = MinIOClient.readParquetFrom@@MinIO(spark, "nyc", "yellow_tripdata_2025-08.parquet")
      println(s"   ✓ Données chargées: ${data.count()} lignes")

      // 2. Préparation des données et feature engineering
      println("\n[2/5] Préparation des données et feature engineering...")
      val preparedData = DataPreparation.prepareFeatures(data)
      println(s"   ✓ Features préparées")

      // Affichage du schéma des features
      println("\n   Schema des features:")
      preparedData.select("features", "label").printSchema()

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

```


#### Short summary: 

empty definition using pc, found symbol in pc: 