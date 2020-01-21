package latis.ops

import latis.data._
import latis.metadata._
import latis.model._
import java.net.URI
//import latis.input.MatrixTextAdapter
import latis.util._
import scala.io.Source
import latis.input.GoesImageReader
import latis.output.Writer

/**
 * Operation on a granule list dataset to get data for each URI.
 */
case class GoesImageReaderOperation() extends UnaryOperation {

  /**
   * Construct a function to convert samples of URIs to samples of image data.
   */
  def makeMapFunction(model: DataType): Sample => Sample = {
    //Define function to map URIs to data samples
    (sample: Sample) => sample match {
      //  assume uri is first in range for now
      //TODO: enforce by projecting only "uri"?
      case Sample(domain, RangeData(Text(uri))) => ???
        //val image = GoesImageReader(new URI(uri)).getDataset // (iy, ix) -> radiance
        //Sample(domain, RangeData(image.data))
    }
  }
  
  override def applyToData(data: SampledFunction, model: DataType): SampledFunction =
    ??? //data.map(makeMapFunction(model))
  
  // wavelength -> (row, column) -> radiance
  override def applyToModel(model: DataType): DataType =
    Function(
      Scalar(Metadata("wavelength") + ("type" -> "int")),
      Function(
        Tuple(
          Scalar(Metadata("row") + ("type" -> "int")), 
          Scalar(Metadata("column") + ("type" -> "int"))
        ),
        Scalar(Metadata("radiance") + ("type" -> "double"))
      )
    )
    
}
