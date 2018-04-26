package latis.input

import java.net.URL
import scala.io.Source
import latis.ops._
import latis.data._
import latis.metadata._
import scala.collection.mutable.ArrayBuffer
import latis.Dataset
import latis.util.HysicsUtils
import java.net.URI

class HysicsSparkReader extends AdaptedDatasetSource {
  
  // Define DataFrame table as URI
  val uri = new URI("hysics")
  
  // Define model
  val x = ScalarType("x")
  val y = ScalarType("y")
  val wavelength = ScalarType("wavelength")
  val domain = TupleType("")(x, y, wavelength)
  val range = ScalarType("value")
  val model = FunctionType("f")(domain, range)
  
  val metadata = Metadata("id" -> "hysics")(model)
  
  val adapter = SparkDataFrameAdapter(model)

}

object HysicsSparkReader {
  
  def apply() = new HysicsSparkReader()

}