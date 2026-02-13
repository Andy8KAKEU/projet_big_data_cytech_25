import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.File
import common.MinIOClient
import java.net.URL
import com.amazonaws.services.s3.{
  AmazonS3,
  AmazonS3Client,
  AmazonS3ClientBuilder
}
import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest}

/** Module de récupération des données NYC Taxi.
  *
  * Ce module télécharge les fichiers Parquet des données de taxi jaune de NYC
  * depuis l'API publique AWS CloudFront et les stocke dans MinIO pour un
  * traitement ultérieur.
  */
object MinIOLoader {

  /** Point d'entrée principal du module de récupération de données.
    *
    * Cette méthode récupère les paramètres de configuration depuis les
    * variables d'environnement, télécharge le fichier Parquet depuis l'URL
    * source et l'uploade vers MinIO.
    *
    * Variables d'environnement utilisées:
    *   - URL_YELLOW_TAXI: URL du fichier Parquet à télécharger
    *   - BUCKET_NAME: Nom du bucket MinIO de destination
    *   - FILE_NAME_BUCKET_FIRST_DEPOSIT: Nom du fichier dans MinIO
    *
    * @param args
    *   Arguments de ligne de commande (non utilisés)
    */
  def main(args: Array[String]): Unit = {

    // Récupération de l'URL source du fichier Parquet
    val url = sys.env.getOrElse(
      "URL_YELLOW_TAXI",
      "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-08.parquet"
    )

    // Récupération du nom du bucket MinIO
    val bucketNAme = sys.env.getOrElse("BUCKET_NAME", "nyc")

    // Récupération du nom de destination du fichier
    val dest = sys.env.getOrElse(
      "FILE_NAME_BUCKET_FIRST_DEPOSIT",
      "yellow_tripdata_2025-08.parquet"
    )

    try {
      // Téléchargement et upload du fichier vers MinIO
      MinIOClient.uploadParquetFileToMinIO(url, bucketNAme, dest)
    } catch {
      case ex: Exception =>
        println(s"Erreur lors du chargement : ${ex.getMessage}")
    }
  }
}
