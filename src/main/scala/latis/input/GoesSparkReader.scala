package latis.input

import scala.io.Source
import latis.metadata._
import latis.Dataset
import java.net.URI

/**
 * 
 * uri is the name of the cached RDD
 */
//case class GoesSparkReader(uri: URI) extends AdaptedDatasetSource {
case class GoesSparkReader() extends AdaptedDatasetSource {
  
  val uri = new URI("goes_image_files")
  
// (y, x, wavelength) -> Rad
  val model = FunctionType(
    TupleType(
      ScalarType("y"),
      ScalarType("x"),
      ScalarType("wavelength")
    ),
    ScalarType("Rad")
  )
  
  val adapter = SparkAdapter(model)

}

object GoesSparkReader {
  
  //def apply() = new GoesSparkReader(new URI("goes_image_files"))

}