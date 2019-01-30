package latis.input

import latis.util.AWSUtils

import java.io.InputStream
import java.net.URI

import scala.concurrent.ExecutionContext

import cats.effect.IO
import fs2.Stream
import fs2.io.readInputStream

class S3StreamSource extends StreamSource {
  
  /**
   * The S3StreamSource supports only URIs with an "s3" scheme.
   */
  def supportsScheme(uriScheme: String): Boolean =
    uriScheme == "s3"
    
  /**
   * Extract S3 bucket and key from a URI of the form:
   *   s3://<bucket>/<key>
   */
  private def parseURI(uri: URI): (String, String) = {
    //TODO: handle errors
    (uri.getHost, uri.getPath.stripPrefix("/"))
  }
    
  
  def getStream(uri: URI): Stream[IO, Byte] = {
    val (bucket, key) = parseURI(uri)
    
    // Provide context info for fs2 
    //TODO: should this use the blockingExecutionContext like UrlStreamSource?
    val ec = ExecutionContext.global
    implicit val cs = IO.contextShift(ec)
   
    //TODO: handle errors
    val s3 = AWSUtils.s3Client.get
    val is: InputStream = s3.getObject(bucket, key).getObjectContent
    val fis = IO(is) 
    readInputStream(fis, 4096, ec)
  }
}
