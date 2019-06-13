package latis.util

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.AnonymousAWSCredentials
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration

object AWSUtils {
  
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

//  val endpoint = new EndpointConfiguration("http://localhost:8001", "us-west-2")
//  val s3 = AmazonS3ClientBuilder
//    .standard
//    .withPathStyleAccessEnabled(true)
//    .withEndpointConfiguration(endpoint)
//    .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
//    .build
}