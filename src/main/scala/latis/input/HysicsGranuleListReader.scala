package latis.input

import java.net.URL
import scala.io.Source
import latis.ops._
import latis.data._
import latis.metadata._
import scala.collection.mutable.ArrayBuffer
import latis.Dataset
import latis.util.HysicsUtils
import latis.util.AWSUtils
import java.net.URI
import latis.util.LatisProperties

case class HysicsGranuleListReader(uri: URI) extends AdaptedDatasetSource {
  
  val model = FunctionType(
    ScalarType("iy"),
    ScalarType("uri")
  )
   
  override def metadata = Metadata("id" -> "hysics_image_files")(model)
    
  def adapter: Adapter = new Adapter() {
    def apply(uri: URI): Data = {
      val base = uri.toString //"s3:/hylatis-hysics-001/des_veg_cloud"
      //val imageCount = LatisProperties.getOrElse("imageCount", "4200").toInt
      // Use image count to compute a stride.
      val stride: Int = 1 //4200 / imageCount
    
      val samples = Iterator.range(1, 4201, stride) map { i =>
        val y = Integer(i)
        val uri = Text(f"${base}/img$i%04d.txt")
        Sample(y, uri)
      }

      Function.fromSamples(samples)
    }
  }
  
}
  
object HysicsGranuleListReader {
  
  def apply() = {
    val defaultURI = "s3:/hylatis-hysics-001/des_veg_cloud"
    val uri = LatisProperties.getOrElse("hysics.base.uri", defaultURI)
    new HysicsGranuleListReader(URI.create(uri))
  }

}