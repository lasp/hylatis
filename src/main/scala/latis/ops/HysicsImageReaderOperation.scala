package latis.ops

import latis.data._
import latis.metadata._
import latis.model._
import java.net.URI
//import latis.input.MatrixTextAdapter
import latis.util._
import scala.io.Source
import latis.input.DatasetSource
import latis.input.HysicsImageReader

/**
 * Operation on a granule list dataset to get data for each URI.
 */
case class HysicsImageReaderOperation() extends Operation {

  def wavelengths: Array[Double] = {
    val defaultURI = "s3://hylatis-hysics-001/des_veg_cloud"
    val uri = new URI(LatisProperties.getOrElse("hysics.base.uri", defaultURI))
    val wuri = URI.create(s"${uri.toString}/wavelength.txt")
    //TODO: fs2 Stream: val is = NetUtils.resolve(wuri).getStream
    val is = wuri.toURL.openStream
    val source = Source.fromInputStream(is)
    val data = source.getLines().next.split(",").map(_.toDouble)
    source.close
    data
  }
  
  /**
   * Read the array of wavelength values from the data file
   * and broadcast for reuse.
   */
  private def broadcastWavelengths() =
    SparkUtils.sparkContext.broadcast(wavelengths) //TODO: add util?

  
  /**
   * Construct a function to convert samples of URIs to samples of image data
   * used to make the hysics data cube: iy -> (ix, wavelength) -> irradiance
   */
  def makeMapFunction(model: DataType): Sample => Sample = {
    //Read and broadcast the wavelength values
    val bcWavelengths = broadcastWavelengths()
    
    //Define function to map URIs to data samples
    (sample: Sample) => sample match {
      // Sample of granule list: iy -> URI
      //TODO: use model to determine sample value for URI
      //  assume uri is first in range for now
      //TODO: enforce by projecting only "uri"?
      case Sample(domain, RangeData(uri: String)) =>
        val ws = bcWavelengths.value
 //val ws = wavelengths
        val image = HysicsImageReader(new URI(uri)).getDataset() // (ix, iw) -> irradiance
        
        //replace iw with wavelength values: (ix, wavelength) -> irradiance
//TODO: use operation, update model
        val sf = image.data map {
          case Sample(DomainData(ix, iw: Int), range) => Sample(DomainData(ix, ws(iw)), range)
//TODO: need to sort samples; ws are descending
        }

        Sample(domain, RangeData(sf))
    }
  }
  
  override def applyToData(data: SampledFunction, model: DataType): SampledFunction =
    data.map(makeMapFunction(model))
  
  // iy -> (ix, wavelength) -> irradiance
//TODO: map f to replace uri with image type
  override def applyToModel(model: DataType): DataType =
    Function(
      Scalar(Metadata("iy") + ("type" -> "int")),
      Function(
        Tuple(
          Scalar(Metadata("ix") + ("type" -> "int")), 
          Scalar(Metadata("wavelength") + ("type" -> "double"))
        ),
        Scalar(Metadata("irradiance") + ("type" -> "double"))
      )
    )
    
}