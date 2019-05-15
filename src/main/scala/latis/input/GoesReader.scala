package latis.input

import java.net.URL
import scala.io.Source
import latis.ops._
import latis.input._
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
import latis.model._

/**
 * Read the Goes granule list dataset, cache it into Spark,
 * and apply operation to read and structure the data.
 * Cache the LaTiS Dataset in memory so we don't have to reload 
 * it into spark each time.
 */
case class GoesReader() extends DatasetReader {
  //NOTE: The native image ordering is consistent with 0-based row-column ordering.
  
  def getDataset: Dataset = {
    // Load the granule list dataset into spark
    //val ds = Dataset.fromName("goes_image_files")   // wavelength -> uri
    val ds: Dataset = {
      val md = Metadata("goes_image_files")
      val model = Function(
        Scalar(Metadata("wavelength") + ("type" -> "double")),
        Scalar(Metadata("uri") + ("type" -> "string"))
      )
      val uri = new URI(LatisProperties.get("goes.base.uri").get)
      val data = (new GoesGranuleListAdapter)(uri)
      Dataset(md, model, data)
        .restructure(RddFunction) //include this to memoize data in the form of a Spark RDD
//.unsafeForce 
    }
    
    val ops: Seq[UnaryOperation] = Seq(
      GoesImageReaderOperation(), // Load data from each granule: wavelength -> (row, column) -> radiance
      //Uncurry()  // Uncurry the dataset: (wavelength, row, column) -> radiance
    )
    
    // Apply Operations
    val ds2 = ops.foldLeft(ds)((ds, op) => op(ds))
        
    // Persist the RDD now that all operations have been applied
    val data = ds2.data match {
      case rf: RddFunction => 
        //TODO: config storage level
        RddFunction(rf.rdd.persist(StorageLevel.MEMORY_AND_DISK_SER))
      case sf => sf //no-op if not an RddFunction
    }

    // Create and rename the new Dataset and add it to the LaTiS CacheManager.
    val ds3 = ds2.copy(data = data).rename("goes")
    ds3.cache()
    ds3
  }
  
}

