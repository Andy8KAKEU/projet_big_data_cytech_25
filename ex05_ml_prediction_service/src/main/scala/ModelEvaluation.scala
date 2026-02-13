import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.ml.evaluation.{
  BinaryClassificationEvaluator,
  MulticlassClassificationEvaluator
}

/** Objet pour l'évaluation du modèle de classification
  */
object ModelEvaluation {

  /** Évalue les performances du modèle de classification
    *
    * @param predictions
    *   DataFrame contenant les prédictions et les vraies labels
    */
  def evaluate(predictions: DataFrame): Unit = {
    println("\n" + "=" * 60)
    println("RÉSULTATS DE L'ÉVALUATION DU MODÈLE")
    println("=" * 60)

    // 1. Matrice de confusion
    printConfusionMatrix(predictions)

    // 2. Métriques de classification
    printClassificationMetrics(predictions)

    // 3. Métriques binaires (AUC, ROC)
    printBinaryMetrics(predictions)

    // 4. Distribution des prédictions
    printPredictionDistribution(predictions)
  }

  /** Affiche la matrice de confusion
    */
  private def printConfusionMatrix(predictions: DataFrame): Unit = {
    println("\n--- Matrice de Confusion ---")

    val tp = predictions.filter("label = 1.0 AND prediction = 1.0").count()
    val tn = predictions.filter("label = 0.0 AND prediction = 0.0").count()
    val fp = predictions.filter("label = 0.0 AND prediction = 1.0").count()
    val fn = predictions.filter("label = 1.0 AND prediction = 0.0").count()

    println(s"""
      |                    Prédit: Non-Tip    Prédit: Tip
      |   Réel: Non-Tip        $tn%-12s   $fp%-12s
      |   Réel: Tip            $fn%-12s   $tp%-12s
    """.stripMargin)

    println(s"   True Positives (TP):  $tp")
    println(s"   True Negatives (TN):  $tn")
    println(s"   False Positives (FP): $fp")
    println(s"   False Negatives (FN): $fn")
  }

  /** Affiche les métriques de classification
    */
  private def printClassificationMetrics(predictions: DataFrame): Unit = {
    println("\n--- Métriques de Classification ---")

    // Accuracy
    val accuracyEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = accuracyEvaluator.evaluate(predictions)
    println(f"   Accuracy (Exactitude):  ${accuracy * 100}%.2f%%")

    // Precision
    val precisionEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedPrecision")
    val precision = precisionEvaluator.evaluate(predictions)
    println(f"   Precision (Précision):  ${precision * 100}%.2f%%")

    // Recall
    val recallEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedRecall")
    val recall = recallEvaluator.evaluate(predictions)
    println(f"   Recall (Rappel):        ${recall * 100}%.2f%%")

    // F1 Score
    val f1Evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("f1")
    val f1 = f1Evaluator.evaluate(predictions)
    println(f"   F1 Score:               ${f1 * 100}%.2f%%")
  }

  /** Affiche les métriques binaires (AUC, ROC)
    */
  private def printBinaryMetrics(predictions: DataFrame): Unit = {
    println("\n--- Métriques Binaires ---")

    // Area Under ROC
    val aucEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC")
    val auc = aucEvaluator.evaluate(predictions)
    println(f"   Area Under ROC (AUC):   ${auc * 100}%.2f%%")

    // Area Under PR
    val auprEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderPR")
    val aupr = auprEvaluator.evaluate(predictions)
    println(f"   Area Under PR:          ${aupr * 100}%.2f%%")
  }

  /** Affiche la distribution des prédictions
    */
  private def printPredictionDistribution(predictions: DataFrame): Unit = {
    println("\n--- Distribution des Prédictions ---")

    val total = predictions.count()
    val predictionDistribution = predictions
      .groupBy("prediction")
      .count()
      .withColumn("percentage", round(col("count") * 100.0 / total, 2))
      .orderBy("prediction")

    predictionDistribution.show(false)
  }

  /** Calcule et affiche des métriques personnalisées
    */
  def calculateCustomMetrics(predictions: DataFrame): Unit = {
    val tp =
      predictions.filter("label = 1.0 AND prediction = 1.0").count().toDouble
    val tn =
      predictions.filter("label = 0.0 AND prediction = 0.0").count().toDouble
    val fp =
      predictions.filter("label = 0.0 AND prediction = 1.0").count().toDouble
    val fn =
      predictions.filter("label = 1.0 AND prediction = 0.0").count().toDouble

    // Sensibilité (Recall pour la classe positive)
    val sensitivity = if (tp + fn > 0) tp / (tp + fn) else 0.0

    // Spécificité
    val specificity = if (tn + fp > 0) tn / (tn + fp) else 0.0

    println(f"\n   Sensitivity (Sensibilité): ${sensitivity * 100}%.2f%%")
    println(f"   Specificity (Spécificité): ${specificity * 100}%.2f%%")
  }
}
