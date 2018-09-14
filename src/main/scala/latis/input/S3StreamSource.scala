package latis.input

import java.net.URI
import java.io.InputStream
import latis.util.AWSUtils

case class S3StreamSource(bucket: String, key: String) extends StreamSource {
  
  def getStream: InputStream = {
    val s3 = AWSUtils.s3Client.get
    s3.getObject(bucket, key).getObjectContent
  }
  //TODO: handle error, Option or Try?
  //TODO: auto closable 
  
}