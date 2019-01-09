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

/**
 * This reader provides a sequence of Hysics image file URLs:
 *   i -> uri
 * The URI used to construct this reader is the base URL to the 
 * directory/bucket where the Hysics images files live.
 * The files are named "img0001.txt" through "img4200.txt".
 */
case class HysicsGranuleListReader(uri: URI) extends AdaptedDatasetSource {
  
  val model = Function(
    Scalar(Metadata("iy") + ("type" -> "int")),  //TODO: use "i" or "index" then rename as needed
    Scalar(Metadata("uri") + ("type" -> "string"))
  )
   
  override def metadata = Metadata(
    "id" -> "hysics_image_files"
  )
    
  def adapter: Adapter = new Adapter() {
    def apply(uri: URI): SampledFunction = {
      val base = uri.toString //"s3:/hylatis-hysics-001/des_veg_cloud"
      val imageCount = LatisProperties.getOrElse("imageCount", "4200").toInt
      // Use image count to compute a stride.
      //TODO: use more suitable operations instead of this property
      val stride: Int = 4200 / imageCount
    
      val samples: Stream[IO, Sample] = Stream.range(1, 4201, stride) map { i =>
        val uri = f"${base}/img$i%04d.txt"
        Sample(DomainData(i), RangeData(uri))
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