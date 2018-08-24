package latis.input

import java.net.URI

import latis.metadata._

/**
 * Expects RDD to be cached in SparkUtil cache.
 * HylatisServer init creates the RDD.
 */
case class HysicsSparkReader() extends AdaptedDatasetSource {
  
  val uri = new URI("hysics")
  
  val model = FunctionType(
    TupleType(
      ScalarType("iy"),
      ScalarType("ix"),
      ScalarType("iw")
    ),
    ScalarType("irradiance")
  )
  
  val adapter = SparkAdapter(model)

}
