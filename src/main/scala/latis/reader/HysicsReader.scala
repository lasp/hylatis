package latis.reader

import java.net.URL
import scala.io.Source
import latis.ops._
import latis.model._
import latis.metadata._
import scala.collection.mutable.ArrayBuffer

/**
 * Read entire contents of a matrix (2D) into a Dataset of type
 *   (row, column) -> value
 * The value will be of type Text.
 * row and column are 1-based Integer indexes, starting in the upper left.
 * Order is row-major (column index varying fastest).
 */
class HysicsReader(dir: String) {
  //TODO: make a Matrix subtype of SampledFunction with matrix semantics
  //TODO: allow value to be any Variable type?
  //TODO: impl as Adapter so we can hand it a model with metadata
  //TODO: optimize with index logic
  
  val delimiter = "," //TODO: parameterize
  
  val xScalar = Integer(id = "x")
  val yScalar = Integer(id = "y")
  val wavelength = Real(id = "wavelength")
  
  val domain = Tuple(yScalar, xScalar, wavelength)
  
  val codomain = Real("value")
  
  val metadata = Metadata("id" -> "hysics")
  val model = Function(Metadata("id" -> "f"))(domain, codomain)
  
  lazy val wavelengths: Array[Double] = {
    val source = Source.fromFile(dir + "wavelength.txt")
    val data = source.getLines().next.split(",").map(_.toDouble)
    source.close
    data
  }
   
  def readImage(image: String): Array[Array[Double]] = {
    val source = Source.fromFile(dir + image)
    val data = source.getLines.map(_.split(delimiter).map(_.toDouble)).toArray
    source.close
    data
  }
  
  def getDataset: Dataset = {
    val range = 2000 until 2010
    val buffer = new ArrayBuffer[Array[Array[Double]]](range.length)
    range.foreach { n =>
      //TODO: deal with n < 1000
      buffer += readImage(s"img$n.txt")
    }
    val data = buffer.toArray
    
    val ny = data.length
    val nx = data(0).length
    val nw = wavelengths.length
    
    val samples: Seq[Sample] = for (
      iy <- 0 until ny;
      ix <- 0 until nx;
      iw <- 0 until nw
    ) yield {
      val y = yScalar.copy(iy.toLong)
      val x = xScalar.copy(ix.toLong)
      val w = wavelength.copy(wavelengths(iw))
      val value = codomain.copy(data(iy)(ix)(iw).toDouble)
      val domain = Tuple(y,x,w)
      Sample(domain, value)
    }
//TODO: can't have null adapter    
    Dataset(metadata, SampledFunction(samples), null)
  }
  
}

object HysicsReader {
  
  def apply() = new HysicsReader("/data_systems/data/test/hylatis/")

  def apply(dir: String): HysicsReader = {
    new HysicsReader(dir)
  }
}