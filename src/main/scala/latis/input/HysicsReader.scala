package latis.input

import java.net.URL
import scala.io.Source
import latis.ops._
import latis.data._
import latis.metadata._
import scala.collection.mutable.ArrayBuffer
import latis.Dataset
import latis.util.HysicsUtils

class HysicsReader(dir: String) extends DatasetSource {
  //TODO: make a Matrix subtype of SampledFunction with matrix semantics
  //TODO: allow value to be any Variable type?
  //TODO: impl as Adapter so we can hand it a model with metadata
  //TODO: optimize with index logic
  
  val delimiter = "," //TODO: parameterize
  
  val lonType = ScalarType("longitude")
  val latType = ScalarType("latitude")
  val wavelength = ScalarType("wavelength")
  val domain = TupleType("")(latType, lonType, wavelength)
  val range = ScalarType("value")
  val ftype = FunctionType("f")(domain, range)
  
  val metadata = Metadata("id" -> "hysics")
  
  private lazy val wavelengths: Array[Double] = {
    val source = Source.fromFile(dir + "wavelength.txt")
    val data = source.getLines().next.split(",").map(_.toDouble)
    source.close
    data
  }
   
  private def readImage(image: String): Array[Array[Double]] = {
    val source = Source.fromFile(dir + image)
    val data = source.getLines.map(_.split(delimiter).map(_.toDouble)).toArray
    source.close
    data
  }
  
  /**
   * Read slit images (x,w) -> f
   * TODO: add lon, lat
   * then logically join on a new axis "y"
   */
  def getDataset(operations: Seq[Operation]): Dataset = {
    //val range = Iterator.range(1, 4201)
    val range = Iterator.range(1, 11)
    
    val samples: Iterator[Sample] = range.flatMap { iy =>
 println(iy)     
      val image: Array[Array[Double]] = readImage(f"img$iy%04d.txt")

      val nx = image.length
      val nw = wavelengths.length

      for (
        ix <- Iterator.range(0, nx);
        iw <- Iterator.range(0, nw)
      ) yield {
        val ll = HysicsUtils.indexToGeo(ix, iy)
        val lon = Scalar(ll._1)
        val lat = Scalar(ll._2)
        val w = Scalar(wavelengths(iw))
        val value = Scalar(image(ix)(iw).toDouble)
        val domain = Tuple(lon, lat, w)
        Sample(domain, value)
      }
    }

    //TODO: apply ops?
    Dataset(ftype, metadata, Function.fromSamples(samples))
  }
}

object HysicsReader {
  
  def apply() = new HysicsReader("/data/hysics/des_veg_cloud/")

  def apply(dir: String): HysicsReader = {
    new HysicsReader(dir)
  }
}