package latis.reader

import java.net.URL
import scala.io.Source
import latis.ops._
import latis.fdm._
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
  
  val domain = Tuple("domain")(Seq(
    Real("y"),
    Real("x"),
    Real("wavelength")
  ))
  
  val codomain = Real("value")
  
  val model = Dataset(Metadata("id" -> "hysics")) {
    Function("f")(domain, codomain)
  }
  
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
    val range = 2000 until 2002
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
      val y = Real("y", iy.toDouble)
      val x = Real("x", ix.toDouble)
      val w = Real("wavelength", wavelengths(iw))
      val value = Real("value", data(iy)(ix)(iw).toDouble)
      val domain = Tuple(y,x,w)
      Sample(domain, value)
    }
    
    Dataset(model.metadata)(SampledFunction(Metadata())(domain, codomain)(samples))
  }
  
}

object HysicsReader {
  
  def apply() = new HysicsReader("/data_systems/data/test/hylatis/")

  def apply(dir: String): HysicsReader = {
    new HysicsReader(dir)
  }
}