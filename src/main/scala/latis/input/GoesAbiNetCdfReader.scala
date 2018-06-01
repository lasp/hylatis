package latis.input

import java.net.URI

import latis.data._
import latis.metadata._ 

/**
 * Proof of concept to show that a simple model can be constructed to extract GOES radiance data from S3.
 * Reader is not aware of UCAR netcdf dependencies, that is left to the adapter.
 */
class GoesAbiNetcdfReader(netCDFUri: String) extends AdaptedDatasetSource {
  
  // Define model
  val uri = new URI(netCDFUri) 
  val x = ScalarType("x")
  val y = ScalarType("y")
  val domain = TupleType("")(y, x)
  val range = ScalarType("Rad")
  val model: FunctionType = FunctionType("f")(domain, range)
  
  val metadata = Metadata("id" -> "goes_16")(model)
  
  val adapter = new GoesAbiNetcdfAdapter(model)
  
  val data: Data = adapter.apply(uri)
  
}



object GoesAbiNetcdfReader {
  
  def apply(netCDFUri: String) = new GoesAbiNetcdfReader(netCDFUri)

}
