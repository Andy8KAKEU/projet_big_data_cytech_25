package common

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.File
import java.net.URL
import com.amazonaws.services.s3.{
  AmazonS3,
  AmazonS3Client,
  AmazonS3ClientBuilder
}
import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest}

/** Client MinIO pour la gestion des opérations de lecture et écriture de
  * fichiers Parquet.
  *
  * Cet objet fournit des méthodes utilitaires pour interagir avec MinIO
  * (stockage S3-compatible) en utilisant Apache Spark pour les opérations de
  * lecture/écriture de fichiers Parquet, ainsi que le SDK AWS pour les
  * opérations de téléchargement direct.
  */
object MinIOClient {

  /** Lit un fichier Parquet depuis MinIO en utilisant Spark.
    *
    * @param spark
    *   Session Spark active
    * @param bucketName
    *   Nom du bucket MinIO
    * @param fileName
    *   Nom du fichier Parquet à lire
    * @return
    *   DataFrame contenant les données du fichier Parquet
    */
  def readParquetFromMinIO(
      spark: SparkSession,
      bucketName: String,
      fileName: String
  ): DataFrame = {
    val filePath = s"s3a://$bucketName/$fileName"
    println(s"Lecture du fichier Parquet depuis $filePath")
    spark.read.parquet(filePath)
  }

  /** Écrit un DataFrame vers MinIO au format Parquet.
    *
    * @param spark
    *   Session Spark active
    * @param dataFrame
    *   DataFrame à écrire
    * @param bucketName
    *   Nom du bucket MinIO de destination
    * @param fileName
    *   Nom du fichier Parquet à créer
    */
  def writeParquetToMinIO(
      spark: SparkSession,
      dataFrame: DataFrame,
      bucketName: String,
      fileName: String
  ): Unit = {
    val filePath = s"s3a://$bucketName/$fileName"
    println(s"Écriture du DataFrame vers Parquet dans $filePath")
    dataFrame.write.mode("overwrite").parquet(filePath)
  }

  /** Télécharge un fichier Parquet depuis une URL HTTP et l'uploade vers MinIO.
    *
    * Cette méthode établit une connexion HTTP vers l'URL source, télécharge le
    * fichier en streaming et l'uploade directement vers MinIO sans stockage
    * intermédiaire local.
    *
    * @param url
    *   URL HTTP du fichier Parquet à télécharger
    * @param bucketName
    *   Nom du bucket MinIO de destination
    * @param key
    *   Clé (nom) du fichier dans MinIO
    */
  def uploadParquetFileToMinIO(
      url: String,
      bucketName: String,
      key: String
  ): Unit = {
    // Créer le client Amazon S3 pour MinIO
    val s3Client: AmazonS3 = createS3Client()

    try {

      // Connexion HTTP
      val connection = new URL(url).openConnection()
      val inputStream = connection.getInputStream

      // Métadonnées
      val metadata = new ObjectMetadata()
      metadata.setContentType("application/octet-stream")

      val contentLength = connection.getContentLengthLong
      if (contentLength > 0) {
        metadata.setContentLength(contentLength)
      }

      // Requête S3
      val putRequest = new PutObjectRequest(
        bucketName,
        key,
        inputStream,
        metadata
      )

      s3Client.putObject(putRequest)
      inputStream.close()
      println(s"Fichier Parquet '$key' uploadé avec succès vers MinIO")

    } catch {
      case e: Exception =>
        println(
          s"Erreur lors du téléchargement du fichier Parquet : ${e.getMessage}"
        )
    }
  }

  /** Crée et configure un client S3 pour interagir avec MinIO.
    *
    * Les paramètres de connexion sont récupérés depuis les variables
    * d'environnement:
    *   - S3_ENDPOINT: URL du serveur MinIO (défaut: http://localhost:9000/)
    *   - MINIO_ROOT_USER: Nom d'utilisateur MinIO (défaut: minio)
    *   - MINIO_ROOT_PASSWORD: Mot de passe MinIO (défaut: minio123)
    *
    * @return
    *   Client AmazonS3 configuré pour MinIO
    */
  def createS3Client(): AmazonS3 = {
    val minioEndpoint =
      sys.env.getOrElse("S3_ENDPOINT", "http://localhost:9000/")
    val accessKey = sys.env.getOrElse("MINIO_ROOT_USER", "minio")
    val secretKey = sys.env.getOrElse("MINIO_ROOT_PASSWORD", "minio123")
    val credentials =
      new com.amazonaws.auth.BasicAWSCredentials(accessKey, secretKey)
    val s3Client = new AmazonS3Client(credentials)
    s3Client.setEndpoint(minioEndpoint)
    s3Client
  }

}
