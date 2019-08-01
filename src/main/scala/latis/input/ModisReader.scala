package latis.input

import latis.data._
import latis.model._
import latis.metadata.Metadata
import latis.ops._
import latis.util._

import java.net.URI

/**
 * Read a MODIS MYD021KM (1 km radiance) HDF-EOS file
 * and generate a spectral data cube by joining all the 
 * available bands. The cube will be curried by band:
 *   band -> (ix, iy) -> radiance
 * and restructured into a Spark RDD.
 * 
 * Note that there is no true wavelength coverage so we 
 * model the spectral dimension with the band number as
 * defined at: https://modis.gsfc.nasa.gov/about/specifications.php
 * Bands 13 and 14 have both a "lo" and "hi" mode so there
 * are bands 13.5 and 14.5 to capture the "hi" modes.
 * 
 * Note that there are no coordinate variables defined for
 * the x (along-track) and y (along-scan) dimensions. The
 * file has longitudes and latitudes (scaled) but they don't
 * define a Cartesian coordinate system.
 */
case class ModisReader() extends DatasetReader {
  
  /**
   * Get the URI for the MODIS data file to read.
   */
  val uri: URI = LatisConfig.get("hylatis.modis.uri") match {
    case Some(s) => new URI(s) //TODO: invalid URI
    case _ => ??? //TODO: uri not defined
  }
      
  /**
   * Define the model for each uncurried component of the
   * spectral dimension.
   *   (band, ix, iy) -> radiance
   */
  val origModel = Function(
    Tuple(
      Scalar("band"), Scalar("ix"), Scalar("iy")
      //Tuple(Metadata("geoIndex"), Scalar("ix"), Scalar("iy"))
    ),
    Scalar("radiance")
  )
  
  /**
   * The spectral segments are found in the following variables
   * in the HDF file.
   */    
  val varNames = List(
    "MODIS_SWATH_Type_L1B/Data_Fields/EV_250_Aggr1km_RefSB",    
    //"MODIS_SWATH_Type_L1B/Data_Fields/EV_500_Aggr1km_RefSB",    
    //"MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_RefSB",    
    //"MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_Emissive",    
  )
  
  /**
   * Construct the final Dataset by building a Dataset for each component,
   * currying them by the band, then unioning them together.
   * Note that this will effectively sort by band with minimal data
   * munging since each large grid will be encapsulated in the range
   * of each of the 38 samples.
   */
  def getDataset: Dataset = {
    
    // Get a Dataset for one chunk of the spectral dimension
    def getDataset(varName: String): Dataset = {
      val data = ModisNetcdfAdapter(varName)(uri)
      val ds = Dataset(Metadata("modis"), origModel, data)
      
      //TODO: put in Spark first? curry should cause repartitioning
      //  potentially expensive but could provide sorting with our partitioner
      
      val ds2 = Curry()(ds)  // band -> (ix, iy) -> radiance
      ds2.restructure(RddFunction)  //put into Spark
      //ds2.unsafeForce  //TODO: broke on Union
    }
    
    // Define the binary operation to union the datasets
//    val join: (Dataset,Dataset) => Dataset = Union().apply
    
    // Union all of the segments into a single Dataset
//    varNames.map(getDataset(_)).reduce(join)
 varNames.map(getDataset(_)).head
  }
}
