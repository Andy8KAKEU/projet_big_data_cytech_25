import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{
  RandomForestClassifier,
  RandomForestClassificationModel
}
import org.apache.spark.ml.PipelineModel

/** Objet pour la définition et l'entraînement du modèle de prédiction
  */
object TipPredictionModel {

  /** Entraîne un modèle de classification Random Forest pour prédire les
    * pourboires
    *
    * @param trainingData
    *   DataFrame contenant les données d'entraînement avec features et label
    * @return
    *   PipelineModel entraîné
    */
  def trainModel(trainingData: DataFrame): PipelineModel = {
    // Configuration du Random Forest Classifier
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      .setProbabilityCol("probability")
      .setRawPredictionCol("rawPrediction")
      // Hyperparamètres du Random Forest
      .setNumTrees(100) // Nombre d'arbres
      .setMaxDepth(10) // Profondeur maximale
      .setMinInstancesPerNode(5) // Minimum d'instances par noeud
      .setFeatureSubsetStrategy("auto") // Stratégie de sélection de features
      .setImpurity("gini") // Mesure d'impureté
      .setSeed(42) // Pour la reproductibilité

    // Créer le pipeline
    val pipeline = new Pipeline()
      .setStages(Array(rf))

    // Entraîner le modèle
    println("   → Entraînement du Random Forest avec 100 arbres...")
    val model = pipeline.fit(trainingData)

    // Afficher l'importance des features si disponible
    displayFeatureImportance(model)

    model
  }

  /** Affiche l'importance des features du modèle Random Forest
    */
  private def displayFeatureImportance(model: PipelineModel): Unit = {
    try {
      val rfModel =
        model.stages.last.asInstanceOf[RandomForestClassificationModel]
      val featureNames = Array(
        "fare_amount",
        "trip_distance",
        "passenger_count",
        "rate_code_id",
        "payment_type_id",
        "extra",
        "mta_tax",
        "tolls_amount",
        "pickup_hour",
        "trip_duration_minutes",
        "congestion_surcharge",
        "airport_fee",
        "cbd_congestion_fee",
        "improvement_surcharge"
      )

      println("\n   → Importance des features:")
      featureNames
        .zip(rfModel.featureImportances.toArray)
        .sortBy(-_._2)
        .foreach { case (name, importance) =>
          println(f"      - $name%-25s: ${importance * 100}%.2f%%")
        }
    } catch {
      case _: Exception =>
        println("   → Impossible d'afficher l'importance des features")
    }
  }

  /** Fait des prédictions sur de nouvelles données
    *
    * @param model
    *   Modèle entraîné
    * @param data
    *   DataFrame contenant les features à prédire
    * @return
    *   DataFrame avec les prédictions
    */
  def predict(model: PipelineModel, data: DataFrame): DataFrame = {
    model.transform(data)
  }
}
