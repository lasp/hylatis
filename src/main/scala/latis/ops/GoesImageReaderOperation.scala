package latis.ops

import latis.model._
import latis.input._
import latis.data._
import latis.metadata._
import java.net.URI
import latis.input.MatrixTextAdapter

/**
 * Operation on a granule list dataset to get data for each URI.
 */
case class GoesImageReaderOperation() extends Operation {
  
  def makeMapFunction(model: DataType): Sample => Sample = (s: Sample) => {
    
    val adapter = GoesAbiNetcdfAdapter() // reuse for each sample //TODO: consider concurrency issues
    //TODO: use HysicsImageReader to get Dataset with the desired model?
    
    s match {
      //TODO: use model to determine sample value for URI
      //  assume uri is first in range for now
      //TODO: enforce by projecting only "uri"?
      case Sample(domain, RangeData(uri: String)) =>
        val data = adapter(new URI(uri))
        Sample(domain, RangeData(data))
    }
  }
  
  // wavelength -> (x, y) -> Rad
  override def applyToModel(model: DataType): DataType = {
    Function(
      Scalar("wavelength"),
      Function(
        Tuple(Scalar("x"), Scalar("y")),
        Scalar("Rad")
      )
    )
  }
}