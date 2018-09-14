package latis.input

import java.net.URI

import latis.metadata._
import latis.model._

/**
 * Expects RDD to be cached in SparkUtil cache.
 * HylatisServer init creates the RDD.
 */
case class HysicsSparkReader() extends AdaptedDatasetSource {
  
  val uri = new URI("hysics")
  
  val model = Function(
    Tuple(
      Scalar("iy"),
      Scalar("ix"),
      Scalar("wavelength")
    ),
    Scalar("irradiance")
  )
  
  val adapter = SparkAdapter(model)

}
