package latis.input

import java.net.URI

import latis.data._
import latis.metadata._ 
import latis.model._

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
  val x = Scalar("x")
  val y = Scalar("y")
  val wavelength = Scalar("wavelength")
  val domain = Tuple(y, x, wavelength)      // y and x are intentionally reversed
  val range = Scalar("Rad")
  val model: Function = Function(domain, range)
  
  override val metadata = Metadata("id" -> "goes_16_multi_channel")
  
  val adapter = new GoesAbiNetcdfAdapter()
  
  val data: SampledFunction = joinDataFromMultipleFiles(colorMap)
  
  /**
   * Iterate across the supplied list of URIs to combine radiance data at different wavelengths.
   * Assume the files are in RGB order.
   */
  def joinDataFromMultipleFiles(colorMap: Map[String, Double]): SampledFunction = {
    val samples = for {
      (netCDFUri, wavelength) <- colorMap
      goesReader = new GoesAbiNetcdfReader(netCDFUri)
      data = goesReader.data
      dataset = Dataset(goesReader.metadata, model, data)
      sample <- dataset.samples
      //Sample(count, Seq(i, j, radiance)) = sample
      (DomainData(i, j), RangeData(radiance)) = sample
    } yield (DomainData(i, j, wavelength), RangeData(radiance))
    //Sample(3, i, j, Real(wavelength), radiance)
    
    StreamingFunction(samples.iterator)
  }
}


object GoesAbiNetcdfMultiChannelReader {
  
  def apply(netCDFUriFiles: List[String]) = new GoesAbiNetcdfMultiChannelReader(netCDFUriFiles)

}
