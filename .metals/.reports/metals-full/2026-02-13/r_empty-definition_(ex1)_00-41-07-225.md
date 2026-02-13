error id: file://<WORKSPACE>/ex01_data_retrieval/src/main/scala/load.scala:
file://<WORKSPACE>/ex01_data_retrieval/src/main/scala/load.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb

found definition using fallback; symbol MinIOClient
offset: 861
uri: file://<WORKSPACE>/ex01_data_retrieval/src/main/scala/load.scala
text:
```scala
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

object MinIOLoader {

  def main(args: Array[String]): Unit = {

    val url = sys.env.getOrElse(
      "URL_YELLOW_TAXI",
      "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-08.parquet"
    )
    val bucketNAme = sys.env.getOrElse("BUCKET_NAME", "nyc")
    val dest = sys.env.getOrElse(
      "FILE_NAME_BUCKET_FIRST_DEPOSIT",
      "yellow_tripdata_2025-08.parquet"
    )

    try {
      MinIOCl@@ient.uploadParquetFileToMinIO(url, bucketNAme, dest)
    } catch {
      case ex: Exception =>
        println(s"Erreur lors du chargement : ${ex.getMessage}")
    }
  }
}

```


#### Short summary: 

empty definition using pc, found symbol in pc: 