package latis.ops

import latis._
import latis.input._
import latis.data._
import latis.metadata._
import java.net.URI
import latis.input.MatrixTextAdapter

/**
 * Operation on a granule list dataset to get data for each URI.
 */
case class GoesImageReaderOperation() extends MapOperation {
  
  def makeMapFunction(model: DataType): Sample => Sample = (s: Sample) => {
    
    val adapter = GoesAbiNetcdfAdapter() // reuse for each sample //TODO: consider concurrency issues
    //TODO: use HysicsImageReader to get Dataset with the desired model?
    
    s match {
      //TODO: use model to determine sample value for URI
      //  assume uri is first in range for now
      //TODO: enforce by projecting only "uri"?
      case Sample(n, ds) => ds(n) match {
        case Text(u) => 
          val data = adapter(new URI(u))
          Sample(n, ds.take(n) :+ data)
      }
    }
  }
  
  // wavelength -> (x, y) -> rad
  override def applyToModel(model: DataType): DataType = {
    FunctionType(
      ScalarType("wavelength"),
      FunctionType(
        TupleType(ScalarType("x"), ScalarType("y")),
        ScalarType("rad")
      )
    )
  }
}