package latis.input

import java.net.URI
import latis.util.AWSUtils
import scala.concurrent.ExecutionContext
import fs2._
import fs2.io._
import cats.effect._
import cats.effect.ContextShift
import cats.implicits._
import java.io.InputStream

case class S3StreamSource(bucket: String, key: String) extends StreamSource {
  
  def getStream: Stream[IO, Byte] = {
    val ec = ExecutionContext.global
    implicit val cs = IO.contextShift(ec)
    
    val s3 = AWSUtils.s3Client.get
    val is: InputStream = s3.getObject(bucket, key).getObjectContent //TODO: handle error
    val fis = IO(is) 
    readInputStream(fis, 4096, ec)
  }
}