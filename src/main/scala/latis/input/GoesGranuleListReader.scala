package latis.input

import java.net.URL
import scala.io.Source
import latis.ops._
import latis.data._
import latis.metadata._
import scala.collection.mutable.ArrayBuffer
import latis.model._
import latis.util.HysicsUtils
import latis.util.AWSUtils
import java.net.URI
import latis.util.LatisProperties

case class GoesGranuleListReader(uri: URI) extends AdaptedDatasetSource {
  
  val wavelengths = Map(0 -> 300, 1 -> 500, 2 -> 700)
  
  val model = Function(
    Scalar("wavelength"),
    Scalar("uri")
  )
   
  override def metadata = Metadata("id" -> "goes_image_files")
    
  def adapter: Adapter = new Adapter() {
    def apply(uri: URI): SampledFunction = {
      val base = uri.toString //"s3:/goes-001"
      val samples = Iterator.range(0, 3) map { i =>
        val y = wavelengths(i)
        val uri = f"${base}/goes$i%04d.nc"
        (DomainData(y), RangeData(uri))
      }

      StreamingFunction(samples)
    }
  }
  
}
  
object GoesGranuleListReader {
  
  def apply() = {
    val defaultURI = "s3://goes-001"
    val uri = LatisProperties.getOrElse("goes.base.uri", defaultURI)
    new GoesGranuleListReader(URI.create(uri))
  }

}