package latis.input

import java.net.URI

import latis.data._
import latis.metadata._ 
import latis.Dataset

/**
 * Proof of concept to show that 3 GOES netcdf files can be combined into a single dataset.
 * Reader is not aware of UCAR netcdf dependencies, that is left to the adapter.
 */
class GoesAbiNetcdfMultiChannelReader(netCDFUriFiles: List[String]) extends AdaptedDatasetSource {
  // wavelengths in nanometers
  val Red = 640.0
  val Green = 530.0
  val Blue = 470.0
  
  val colors = List(Red, Green, Blue)
  
  val colorMap = netCDFUriFiles.zip(colors).toMap
  
  // Define model
  // (y, x, wavelength) -> Rad
  val uri = new URI("") 
  val x = ScalarType("x")
  val y = ScalarType("y")
  val wavelength = ScalarType("wavelength")
  val domain = TupleType("")(y, x, wavelength)      // y and x are intentionally reversed
  val range = ScalarType("Rad")
  val model: FunctionType = FunctionType("f")(domain, range)
  
  val metadata = Metadata("id" -> "goes_16_multi_channel")(model)
  
  val adapter = new GoesAbiNetcdfAdapter(model)
  
  val data: Data = joinDataFromMultipleFiles(colorMap)
  
  /**
   * Iterate across the supplied list of URIs to combine radiance data at different wavelengths.
   * Assume the files are in RGB order.
   */
  def joinDataFromMultipleFiles(colorMap: Map[String, Double]): Data = {
    val samples = for {
      (netCDFUri, wavelength) <- colorMap
      goesReader = new GoesAbiNetcdfReader(netCDFUri)
      data = goesReader.data
      dataset = Dataset(goesReader.metadata, data)
      sample <- dataset.samples
      Sample(count, Seq(i, j, radiance)) = sample
    } yield Sample(3, i, j, Real(wavelength), radiance)
    
    val dataSet = Dataset(metadata, Function.fromSamples(samples.iterator))
    dataSet.data
  }
}


object GoesAbiNetcdfMultiChannelReader {
  
  def apply(netCDFUriFiles: List[String]) = new GoesAbiNetcdfMultiChannelReader(netCDFUriFiles)

}
