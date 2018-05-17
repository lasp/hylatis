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
  
  // (y, x, wavelength) -> irradiance
  val model = FunctionType("f")(
    TupleType("")(
      ScalarType("y"),
      ScalarType("x"),
      ScalarType("wavelength")
    ),
    ScalarType("irradiance")
  )
  
  val metadata = Metadata("id" -> "hysics")(model)
  
  val adapter = SparkAdapter(model)

}

object HysicsSparkReader {
  
  def apply() = new HysicsSparkReader()

}