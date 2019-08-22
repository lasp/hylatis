package latis.util

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.AnonymousAWSCredentials
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import java.net.URI
import java.io.File
import java.io.FileOutputStream

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
    if (uri.getScheme == "s3") (uri.getHost, uri.getPath.drop(1))
    else ??? //TODO: error
  }


//  val endpoint = new EndpointConfiguration("http://localhost:8001", "us-west-2")
//  val s3 = AmazonS3ClientBuilder
//    .standard
//    .withPathStyleAccessEnabled(true)
//    .withEndpointConfiguration(endpoint)
//    .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
//    .build
}