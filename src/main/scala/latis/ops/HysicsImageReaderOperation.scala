package latis.ops

import latis._
import latis.data._
import latis.metadata._
import java.net.URI
import latis.input.MatrixTextAdapter
import latis.util._
import latis.input.URIResolver
import scala.io.Source
import latis.input.DatasetSource
import latis.input.HysicsImageReader

/**
 * Operation on a granule list dataset to get data for each URI.
 */
case class HysicsImageReaderOperation() extends MapOperation {
  
  /**
   * Read the array of wavelength values from the data file
   * and broadcast for reuse.
   */
  private def broadcastWavelengths() = {
    def wavelengths: Array[Double] = {
      val defaultURI = "s3://hylatis-hysics-001/des_veg_cloud"
      val uri = new URI(LatisProperties.getOrElse("hysics.base.uri", defaultURI))
      val wuri = URI.create(s"${uri.toString}/wavelength.txt")
      val is = URIResolver.resolve(wuri).getStream
      val source = Source.fromInputStream(is)
      val data = source.getLines().next.split(",").map(_.toDouble)
      source.close
      data
    }
    SparkUtils.getSparkSession.sparkContext.broadcast(wavelengths) //TODO: add util?
  }
  
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
      case Sample(n, ds) => ds(n) match {
        case Text(uri) => 
          val ws = bcWavelengths.value
          val image = HysicsImageReader(new URI(uri)).getDataset() // (ix, iw) -> irradiance
          //replace iw with wavelength values
          val samples = image.samples map {
            case Sample(2, Seq(ix, Index(iw), f)) => Sample(2, Seq(ix, Real(ws(iw)), f))
          }
          //make Function data for these samples: (ix, wavelength) -> irradiance
          val fd = StreamingFunction(samples)
           
          Sample(n, ds.take(n) :+ fd) // iy -> (ix, wavelength) -> irradiance
      }
    }
  }
  
  // iy -> (ix, wavelength) -> irradiance
  override def applyToModel(model: DataType): DataType = {
    FunctionType(
      ScalarType("iy"),
      FunctionType(
        TupleType(ScalarType("ix"), ScalarType("wavelength")),
        ScalarType("irradiance")
      )
    )
  }
}