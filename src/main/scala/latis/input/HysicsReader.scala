package latis.input

import java.net.URL
import scala.io.Source
import latis.ops._
import latis.data._
import latis.metadata._
import scala.collection.mutable.ArrayBuffer
import latis.util.HysicsUtils
import latis.util.AWSUtils
import java.net.URI
import latis.util.LatisProperties
import latis.model.Dataset
import latis.util.CacheManager
import org.apache.spark.storage.StorageLevel

/**
 * Read the Hysics granule list dataset, put it into Spark,
 * and apply operation to read and structure the data.
 * Cache the RDD and the LaTiS Dataset so we don't have to reload 
 * it into spark each time.
 */
case class HysicsReader() extends DatasetReader {
  //TODO: make a DatasetResolver, see HysicsWavelengths
  //  or reader of any given set of hysics files
  
  def getDataset: Dataset = {
    // Load the granule list dataset into spark
    val reader = HysicsGranuleListReader() // hysics_image_files
    // iy -> uri
    val ds = reader.getDataset
      //TODO: use config option to specify whether to use spark
      .unsafeForce //causes latis to use the MemoizedFunction since StreamFunction is not complete
 //     .restructure(RddFunction) //memoize data in the form of a Spark RDD
      
      
    // Get the dataset of Hysics wavelengths: iw -> wavelength
    val wds = HysicsWavelengths()
    //TODO: cache to spark via broadcast?

      
    // Define operations to be applied to this dataset to complete the cube
    val ops: Seq[UnaryOperation] = Seq(
      HysicsImageReaderOperation(), // Load data from each granule
      Uncurry()  // Uncurry the dataset: (iy, ix, iw) -> irradiance
    )
    
    // Apply Operations
    val ds2 = ops.foldLeft(ds)((ds, op) => op(ds))
    
    // Substitue wavelength values: (iy, ix, wavelength) -> irradiance
    //TODO: handle binary operations better
    val ds3 = Substitution()(ds2, wds)
    
    // Persist the RDD now that all operations have been applied
    val data = ds3.data match {
      case rf: RddFunction => 
        //TODO: config storage level
        RddFunction(rf.rdd.persist(StorageLevel.MEMORY_AND_DISK_SER))
      case sf => sf //no-op if not an RddFunction
    }

    // Create and rename the new Dataset and add it to the LaTiS CacheManager.
    val ds4 = ds3.copy(data = data).rename("hysics")
    ds4.cache()
    ds4
  }
  
}

