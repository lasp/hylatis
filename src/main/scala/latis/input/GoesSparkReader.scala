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

/**
 * 
 * uri is the name of the cached RDD
 */
//case class GoesSparkReader(uri: URI) extends AdaptedDatasetSource {
case class GoesSparkReader() extends AdaptedDatasetSource {
  //TODO: dynamic loading not working with args
  
  val uri = new URI("goes_image_files")
  
// (y, x, wavelength) -> rad
  val model = FunctionType(
    TupleType(
      ScalarType("y"),
      ScalarType("x"),
      ScalarType("wavelength")
    ),
    ScalarType("rad")
  )
  
  val adapter = SparkAdapter(model)

}

object GoesSparkReader {
  
  //def apply() = new GoesSparkReader(new URI("goes_image_files"))

}