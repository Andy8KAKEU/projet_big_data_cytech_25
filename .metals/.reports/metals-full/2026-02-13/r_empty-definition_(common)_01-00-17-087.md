error id: file://<WORKSPACE>/common/src/main/scala/common/MinIOClient.scala:
file://<WORKSPACE>/common/src/main/scala/common/MinIOClient.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -ys.
	 -scala/Predef.ys.
offset: 2487
uri: file://<WORKSPACE>/common/src/main/scala/common/MinIOClient.scala
text:
```scala
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

object MinIOClient {

  // Fonction pour lire un fichier Parquet depuis MinIO en utilisant Spark
  def readParquetFromMinIO(
      spark: SparkSession,
      bucketName: String,
      fileName: String
  ): DataFrame = {
    val filePath = s"s3a://$bucketName/$fileName"
    println(s"Lecture du fichier Parquet depuis $filePath")
    spark.read.parquet(filePath)
  }

  // Fonction pour écrire un DataFrame vers MinIO en format Parquet
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

  // Fonction pour uploader un fichier http Parquet vers MinIO
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

  // Fonction pour créer un client S3 pour MinIO
  def createS3Client(): AmazonS3 = {
    val minioEndpoint = sys.env.getOrElse("S3_ENDPOINT", "http://localhost:9000/")
    val accessKey = y@@s.env.getOrElse("MINIO_ROOT_USER", "minio")
    val secretKey = "minio123"
    val credentials =
      new com.amazonaws.auth.BasicAWSCredentials(accessKey, secretKey)
    val s3Client = new AmazonS3Client(credentials)
    s3Client.setEndpoint(minioEndpoint)
    s3Client
  }

}

```


#### Short summary: 

empty definition using pc, found symbol in pc: 