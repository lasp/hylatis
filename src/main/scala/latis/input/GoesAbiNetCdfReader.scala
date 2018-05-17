package latis.input

import java.net.URI

import latis.data._
import latis.metadata._ 

/**
 * Proof of concept to show that a simple model can be constructed to extract GOES radiance data from S3.
 * Reader is not aware of UCAR netcdf dependencies, that is left to the adapter.
 */
class GoesAbiNetcdfReader extends AdaptedDatasetSource {
  val uri = new URI("file:///Users/pepf5062/Downloads/AwsTest/OR_ABI-L1b-RadF-M3C16_G16_s20180711200421_e20180711211199_c20180711211258.nc")
  //val uri = new URI("s3://noaa-goes16/ABI-L1b-RadF/2018/071/12/OR_ABI-L1b-RadF-M3C16_G16_s20180711200421_e20180711211199_c20180711211258.nc")
  // Define model
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
  
  def apply() = new GoesAbiNetcdfReader()

}
