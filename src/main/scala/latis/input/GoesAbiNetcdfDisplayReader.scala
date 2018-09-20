package latis.input

import java.net.URI

import latis.data._
import latis.metadata._ 
import latis.model._ 

/**
 * Proof of concept to extract GOES radiance data from S3 and to display a in color using a custom color table.
 * Reader is not aware of UCAR netcdf dependencies, that is left to the adapter.
 */
class GoesAbiNetcdfDisplayReader(netCDFUri: String) extends AdaptedDatasetSource {
  
  // Define model
  val uri = new URI(netCDFUri) 
  val x = Scalar("x")
  val y = Scalar("y")
  val domain = Tuple(y, x)      // y and x are intentionally reversed
  val range = Scalar("Rad")
  val model: Function = Function(domain, range)
  
  override val metadata = Metadata("id" -> "goes_16")
  
  val adapter = new GoesAbiNetcdfDisplayAdapter(model)
  
  val data: SampledFunction = adapter.apply(uri)
  
}



object GoesAbiNetcdfDisplayReader {
  
  def apply(netCDFUri: String) = new GoesAbiNetcdfDisplayReader(netCDFUri)

}
