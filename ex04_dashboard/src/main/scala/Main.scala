import common.SparkConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {

  // Répertoire de sortie pour les KPIs
  val outputDir = "ex04_dashboard/output"

  def main(args: Array[String]): Unit = {

    val sparkConfig = SparkConfig("Dashboard")
    val jdbcUrl = sparkConfig.jdbcUrl
    val connectionProperties = sparkConfig.connectionProperties
    val spark = sparkConfig.createSession()
    import spark.implicits._

    // Charger les tables
    val fact_trips =
      spark.read.jdbc(jdbcUrl, "fact_trips", connectionProperties)
    val dim_vendor =
      spark.read.jdbc(jdbcUrl, "dim_vendor", connectionProperties)
    val dim_rate_code =
      spark.read.jdbc(jdbcUrl, "dim_rate_code", connectionProperties)
    val dim_payment_type =
      spark.read.jdbc(jdbcUrl, "dim_payment_type", connectionProperties)
    val dim_date = spark.read.jdbc(jdbcUrl, "dim_date", connectionProperties)
    val dim_location =
      spark.read.jdbc(jdbcUrl, "dim_location", connectionProperties)

    println("=== Calcul des KPIs ===")

    // ============================================
    // KPI 1: Chiffre d'affaires total
    // ============================================
    val kpi1_total_revenue = fact_trips
      .agg(sum("total_amount").as("total_revenue"))
    saveKpi(kpi1_total_revenue, "kpi1_total_revenue")
    println("KPI 1 - Chiffre d'affaires total:")
    kpi1_total_revenue.show()

    // ============================================
    // KPI 2: Nombre total de courses
    // ============================================
    val kpi2_total_trips = fact_trips
      .agg(count("*").as("total_trips"))
    saveKpi(kpi2_total_trips, "kpi2_total_trips")
    println("KPI 2 - Nombre total de courses:")
    kpi2_total_trips.show()

    // ============================================
    // KPI 3: Revenu moyen par course
    // ============================================
    val kpi3_avg_revenue = fact_trips
      .agg(avg("total_amount").as("avg_revenue_per_trip"))
    saveKpi(kpi3_avg_revenue, "kpi3_avg_revenue")
    println("KPI 3 - Revenu moyen par course:")
    kpi3_avg_revenue.show()

    // ============================================
    // KPI 4: Distance moyenne par course
    // ============================================
    val kpi4_avg_distance = fact_trips
      .agg(avg("trip_distance").as("avg_distance"))
    saveKpi(kpi4_avg_distance, "kpi4_avg_distance")
    println("KPI 4 - Distance moyenne par course:")
    kpi4_avg_distance.show()

    // ============================================
    // KPI 5: Durée moyenne des courses (en minutes)
    // ============================================
    val kpi5_avg_duration = fact_trips
      .withColumn(
        "duration_minutes",
        (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(
          col("pickup_datetime")
        )) / 60
      )
      .agg(avg("duration_minutes").as("avg_duration_minutes"))
    saveKpi(kpi5_avg_duration, "kpi5_avg_duration")
    println("KPI 5 - Durée moyenne des courses (minutes):")
    kpi5_avg_duration.show()

    // ============================================
    // KPI 6: Pourboire moyen par course
    // ============================================
    val kpi6_avg_tip = fact_trips
      .agg(avg("tip_amount").as("avg_tip"))
    saveKpi(kpi6_avg_tip, "kpi6_avg_tip")
    println("KPI 6 - Pourboire moyen par course:")
    kpi6_avg_tip.show()

    // ============================================
    // KPI 7: Répartition des moyens de paiement
    // ============================================
    val kpi7_payment_distribution = fact_trips
      .groupBy("payment_type_id")
      .agg(count("*").as("trip_count"))
      .join(dim_payment_type, Seq("payment_type_id"), "left")
      .select("payment_type_id", "payment_description", "trip_count")
      .orderBy(desc("trip_count"))
    saveKpi(kpi7_payment_distribution, "kpi7_payment_distribution")
    println("KPI 7 - Répartition des moyens de paiement:")
    kpi7_payment_distribution.show()

    // ============================================
    // KPI 8: Nombre de courses par heure de la journée
    // ============================================
    val kpi8_trips_by_hour = fact_trips
      .withColumn("hour", hour(col("pickup_datetime")))
      .groupBy("hour")
      .agg(count("*").as("trip_count"))
      .orderBy("hour")
    saveKpi(kpi8_trips_by_hour, "kpi8_trips_by_hour")
    println("KPI 8 - Nombre de courses par heure:")
    kpi8_trips_by_hour.show(24)

    // ============================================
    // KPI 9: Revenu mensuel
    // ============================================
    val kpi9_monthly_revenue = fact_trips
      .withColumn("year", year(col("pickup_datetime")))
      .withColumn("month", month(col("pickup_datetime")))
      .groupBy("year", "month")
      .agg(sum("total_amount").as("monthly_revenue"))
      .orderBy("year", "month")
    saveKpi(kpi9_monthly_revenue, "kpi9_monthly_revenue")
    println("KPI 9 - Revenu mensuel:")
    kpi9_monthly_revenue.show()

    // ============================================
    // KPI 10: Top 10 zones de pickup
    // ============================================
    val kpi10_top_pickup_zones = fact_trips
      .groupBy("pu_location_id")
      .agg(count("*").as("trip_count"))
      .orderBy(desc("trip_count"))
      .limit(10)
    saveKpi(kpi10_top_pickup_zones, "kpi10_top_pickup_zones")
    println("KPI 10 - Top 10 zones de pickup:")
    kpi10_top_pickup_zones.show()

    println(
      "=== Tous les KPIs ont été calculés et sauvegardés dans " + outputDir + " ==="
    )

    spark.stop()
  }

  /** Sauvegarde un DataFrame KPI en format JSON
    */
  def saveKpi(df: DataFrame, name: String): Unit = {
    df.coalesce(1)
      .write
      .mode("overwrite")
      .json(s"$outputDir/$name")
  }
}
