package latis.input

import java.net.URL
import scala.io.Source
import latis.ops._
import latis.data._
import latis.metadata._
import scala.collection.mutable.ArrayBuffer
import latis.model._
import latis.util.HysicsUtils
//import latis.util.AWSUtils
import java.net.URI
import latis.util.LatisProperties
import fs2._
import cats.effect.IO

case class HysicsGranuleListReader(uri: URI) extends AdaptedDatasetSource {
  
  val model = Function(
    Metadata("foo" -> "bar"),
    Scalar("iy"),
    Scalar("uri")
  )
   
  override def metadata = Metadata(
    "id" -> "hysics_image_files"
  )
    
  def adapter: Adapter = new Adapter() {
    def apply(uri: URI): SampledFunction = {
      val base = uri.toString //"s3:/hylatis-hysics-001/des_veg_cloud"
      val imageCount = LatisProperties.getOrElse("imageCount", "4200").toInt
      // Use image count to compute a stride.
      val stride: Int = 4200 / imageCount
    
      val samples: Stream[IO, Sample] = Stream.range(1, 4201, stride) map { i =>
        val uri = f"${base}/img$i%04d.txt"
        (DomainData(i), RangeData(uri))
      }

      StreamFunction(samples)
    }
  }
  
}
  
object HysicsGranuleListReader {
  
  def apply() = {
    val defaultURI = "s3://hylatis-hysics-001/des_veg_cloud"
    val uri = LatisProperties.getOrElse("hysics.base.uri", defaultURI)
    new HysicsGranuleListReader(URI.create(uri))
  }

}