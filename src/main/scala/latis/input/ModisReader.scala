package latis.input

import latis.data._
import latis.model._
import latis.metadata.Metadata
import latis.ops._
import latis.util._

import java.net.URI
import org.apache.spark.storage.StorageLevel
import latis.dataset.Dataset

/**
 * Read a MODIS MYD021KM (1 km radiance) HDF-EOS file
 * and generate a spectral data cube by joining all the 
 * available bands. The geo location data will be substituted 
 * into the grid's domain set:
 *   band -> (longitude, latitude) -> radiance
 *   
 * Each band is defined as a granule in the ModisGranuleListReader.
 * This granule list is put into a Spark RDD and the
 * ModisBandReaderOperation is applied to read the 2D grid for each
 * band.
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
 * define a Cartesian coordinate system. The "MYD03" files
 * contain the full resolution longitude and latitude data
 * as provided by the ModisGeolocationReader. It is used
 * via substitution. Note that the resulting grid does not
 * adhere to natural ordering since the original data is a swath.
 */
case class ModisReader() { //extends DatasetReader {
  
  ///**
  // * Construct the Dataset by creating a granule list Dataset
  // * with URIs representing a 2D grid for a given band.
  // * Apply ModisBandReaderOperation to replace each URI
  // * with the grid then apply ModisGeoSub to replace the index
  // * domain with longitude and latitude.
  // */
  //def getDataset: Dataset = {
  //  val granules = ModisGranuleListReader().getDataset.restructure(RddFunction) // band -> uri
  //
  //  val ops: Seq[UnaryOperation] = Seq(
  //    ModisBandReaderOperation(),  //band -> (ix, iy) -> radiance
  //    ModisGeoSub()                //band -> (longitude, latitude) -> radiance
  //  )
  //
  //  // Apply Operations
  //  val ds = ops.foldLeft(granules)((ds, op) => op(ds))
  //
  //  // Persist the RDD now that all operations have been applied
  //  val data = ds.data match {
  //    case rf: RddFunction =>
  //      //TODO: config storage level
  //      RddFunction(rf.rdd.persist(StorageLevel.MEMORY_AND_DISK_SER))
  //    case sf => sf //no-op if not an RddFunction
  //  }
  //
  //  // Create and rename the new Dataset and add it to the LaTiS CacheManager.
  //  val ds1 = ds.copy(data = data).rename("modis")
  //  ds1.cache()
  //  ds1
  //}
}
