package latis.util

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.AnonymousAWSCredentials
import com.amazonaws.services.s3._
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import java.net.URI
import java.io.File
import java.io.FileOutputStream
import com.amazonaws.services.s3.AmazonS3URI
import java.nio.file.Files

object AWSUtils {
  
  //TODO: do we really want an option here?
  val s3Client = for {
    endpoint <- LatisConfig.get("hylatis.aws.endpoint")
    region <- LatisConfig.get("hylatis.aws.region")
  } yield {
    AmazonS3ClientBuilder
      .standard
      //.withRegion(region)
      .withPathStyleAccessEnabled(true) // Needed for S3Mock
      .withEndpointConfiguration(new EndpointConfiguration(endpoint, region))
      .build
  }
  
  /**
   * Return the bucket and key of a S3 object 
   * from an S3 URI.
   */
  def parseS3URI(uri: URI): (String, String) = {
    val s3uri = new AmazonS3URI(uri)
    (s3uri.getBucket, s3uri.getKey)
  }
  
  def copyS3ObjectToFile(uri: URI, file: File): Unit = {
    val (bucket, key) = parseS3URI(uri)
    val s3is = AWSUtils.s3Client.get.getObject(bucket, key).getObjectContent
    Files.createDirectories(file.toPath.getParent)
    Files.copy(s3is, file.toPath)
    s3is.close
  }


//  val endpoint = new EndpointConfiguration("http://localhost:8001", "us-west-2")
//  val s3 = AmazonS3ClientBuilder
//    .standard
//    .withPathStyleAccessEnabled(true)
//    .withEndpointConfiguration(endpoint)
//    .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
//    .build
}