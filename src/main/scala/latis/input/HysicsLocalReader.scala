package latis.input

import java.net.URL
import scala.io.Source
import latis.ops._
import latis.data._
import latis.metadata._
import scala.collection.mutable.ArrayBuffer
import latis.Dataset
import latis.util.HysicsUtils

class HysicsLocalReader(dir: String) extends DatasetSource {
  //TODO: make a Matrix subtype of SampledFunction with matrix semantics
  //TODO: allow value to be any Variable type?
  //TODO: impl as Adapter so we can hand it a model with metadata
  //TODO: optimize with index logic
  
  val delimiter = "," //TODO: parameterize
  
  // Define model
  val xType = ScalarType("x")
  val yType = ScalarType("y")
  val wavelength = ScalarType("wavelength")
  val domain = TupleType("")(xType, yType, wavelength)
  val range = ScalarType("value")
  val ftype = FunctionType("f")(domain, range)
  
  val metadata = Metadata("id" -> "hysics")(ftype)
  
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
val range = Iterator.range(100, 110)
    
    val samples: Iterator[Sample] = range.flatMap { iy =>
      val image: Array[Array[Double]] = readImage(f"img$iy%04d.txt")

      val nx = image.length
      val nw = wavelengths.length

      for (
        ix <- Iterator.range(0, nx);
        iw <- Iterator.range(0, nw)
      ) yield {
        //val ll = HysicsUtils.indexToGeo(ix, iy)
        val ll = HysicsUtils.indexToXY(ix, iy) //leave in native Cartesian x/y space
        val lon = Scalar(ll._1)
        val lat = Scalar(ll._2)
        val w = Scalar(wavelengths(iw))
        val value = Scalar(image(ix)(iw).toDouble)
        val domain = Tuple(lon, lat, w)
        Sample(domain, value)
      }
    }

    //TODO: apply ops?
    Dataset(metadata, Function.fromSamples(samples))
  }
}

object HysicsLocalReader {
  
  def apply() = new HysicsLocalReader("/data/hysics/des_veg_cloud/")

  def apply(dir: String): HysicsLocalReader = {
    new HysicsLocalReader(dir)
  }
}