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
//case class HysicsSparkReader(uri: URI) extends AdaptedDatasetSource {
case class HysicsSparkReader() extends AdaptedDatasetSource {
  //TODO: dynamic loading not working with args
  
  val uri = new URI("hysics_image_files")
  
//  // (y, x, wavelength) -> irradiance
  val model = FunctionType(
    TupleType(
      ScalarType("iy"),
      ScalarType("ix"),
      ScalarType("iw")
    ),
    ScalarType("irradiance")
  )
//  val model = FunctionType(
//    ScalarType("iy"),
//    ScalarType("uri")
//  )
  
  val adapter = SparkAdapter(model)

}

object HysicsSparkReader {
  
  //def apply() = new HysicsSparkReader(new URI("hysics_image_files"))

}