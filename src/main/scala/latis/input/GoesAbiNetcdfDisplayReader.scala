package latis.input

import java.net.URI

import latis.data._
import latis.metadata._ 

/**
 * Proof of concept to extract GOES radiance data from S3 and to display a in color using a custom color table.
 * Reader is not aware of UCAR netcdf dependencies, that is left to the adapter.
 */
class GoesAbiNetcdfDisplayReader(netCDFUri: String) extends AdaptedDatasetSource {
  
  // Define model
  val uri = new URI(netCDFUri) 
  val x = ScalarType("x")
  val y = ScalarType("y")
  val domain = TupleType("")(y, x)      // y and x are intentionally reversed
  val range = ScalarType("Rad")
  val model: FunctionType = FunctionType("f")(domain, range)
  
  override val metadata = Metadata("id" -> "goes_16")(model)
  
  val adapter = new GoesAbiNetcdfDisplayAdapter(model)
  
  val data: Data = adapter.apply(uri)
  
}



object GoesAbiNetcdfDisplayReader {
  
  def apply(netCDFUri: String) = new GoesAbiNetcdfDisplayReader(netCDFUri)

}
