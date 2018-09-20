package latis.input

import java.net.URI

import latis.data._
import latis.metadata._ 
import latis.model._ 

/**
 * Proof of concept to show that a simple model can be constructed to extract GOES radiance data from S3.
 * Reader is not aware of UCAR netcdf dependencies, that is left to the adapter.
 */
class GoesAbiNetcdfReader(netCDFUri: String) extends AdaptedDatasetSource {
  
  // Define model
  val uri = new URI(netCDFUri) 
  val x = Scalar("x")
  val y = Scalar("y")
  val domain = Tuple(y, x)      // y and x are intentionally reversed
  val range = Scalar("Rad")
  val model: Function = Function(domain, range)
  
  override val metadata = Metadata("id" -> "goes_16")
  
  val adapter = new GoesAbiNetcdfAdapter()
  
  val data: SampledFunction = adapter.apply(uri)
  
}


object GoesAbiNetcdfReader {
  
  def apply(netCDFUri: String) = new GoesAbiNetcdfReader(netCDFUri)

}
