package common

import org.apache.spark.sql.SparkSession

/** Classe de configuration Spark réutilisable. Définit toutes les
  * configurations nécessaires sans initialiser le SparkSession. Chaque module
  * peut instancier cette classe et appeler createSession() pour obtenir une
  * session.
  */
class SparkConfig(
    appName: String = sys.env.getOrElse("SPARK_APP_NAME", "SparkApp"),
    master: String = sys.env.getOrElse(
      "SPARK_MASTER",
      "local[*]"
    ), // spark://spark-master:7077
    logLevel: String = sys.env.getOrElse("SPARK_LOG_LEVEL", "WARN")
) {

  // Configuration S3/MinIO
  private val s3AccessKey = sys.env.getOrElse("MINIO_ROOT_USER", "minio")
  private val s3SecretKey = sys.env.getOrElse("MINIO_ROOT_PASSWORD", "minio123")
  private val s3Endpoint =
    sys.env.getOrElse("S3_ENDPOINT", "http://localhost:9000/")
  private val s3PathStyleAccess =
    sys.env.getOrElse("S3_PATH_STYLE_ACCESS", "true")
  private val s3SslEnabled = sys.env.getOrElse("S3_SSL_ENABLED", "false")
  private val s3MaxAttempts = sys.env.getOrElse("S3_MAX_ATTEMPTS", "1")
  private val s3EstablishTimeout =
    sys.env.getOrElse("S3_ESTABLISH_TIMEOUT", "6000")
  private val s3ConnectionTimeout =
    sys.env.getOrElse("S3_CONNECTION_TIMEOUT", "5000")

  // Configuration JDBC PostgreSQL
  val jdbcUrl: String = sys.env.getOrElse(
    "POSTGRES_URL",
    "jdbc:postgresql://localhost:5432/nyc_taxi_dw"
  )

  lazy val connectionProperties: java.util.Properties = {
    val props = new java.util.Properties()
    props.setProperty("user", sys.env.getOrElse("POSTGRES_USER", "psg"))
    props.setProperty(
      "password",
      sys.env.getOrElse("POSTGRES_PASSWORD", "psg123")
    )
    props.setProperty("driver", "org.postgresql.Driver")
    props
  }

  /** Crée et retourne une nouvelle SparkSession avec toutes les configurations.
    */
  def createSession(): SparkSession = {
    val session = SparkSession
      .builder()
      .appName(appName)
      .master(master)
      .config("fs.s3a.access.key", s3AccessKey)
      .config("fs.s3a.secret.key", s3SecretKey)
      .config("fs.s3a.endpoint", s3Endpoint)
      .config("fs.s3a.path.style.access", s3PathStyleAccess)
      .config("fs.s3a.connection.ssl.enable", s3SslEnabled)
      .config("fs.s3a.attempts.maximum", s3MaxAttempts)
      .config("fs.s3a.connection.establish.timeout", s3EstablishTimeout)
      .config("fs.s3a.connection.timeout", s3ConnectionTimeout)
      .getOrCreate()

    session.sparkContext.setLogLevel(logLevel)
    session
  }
}

/** Objet compagnon avec des valeurs par défaut pour un accès facile.
  */
object SparkConfig {

  /** Crée une configuration avec les valeurs par défaut.
    */
  def apply(): SparkConfig = new SparkConfig()

  /** Crée une configuration avec un nom d'application personnalisé.
    */
  def apply(appName: String): SparkConfig = new SparkConfig(appName = appName)

  /** Crée une configuration avec un nom d'application et un master
    * personnalisés.
    */
  def apply(appName: String, master: String): SparkConfig =
    new SparkConfig(appName = appName, master = master)
}
