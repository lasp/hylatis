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

/**
 * Read the Hysics granule list dataset, put it into Spark,
 * and apply operation to read and structure the data.
 * Cache the RDD and the LaTiS Dataset so we don't have to reload 
 * it into spark each time.
 */
case class HysicsReader() extends DatasetReader {
  /*
   * TODO: DatasetReader is used with ServiceLoader, not needed here since this is specific
   * define as HysicsDataset?
   */
  
  /*
   * TODO: focus on getting data cube persisted on s3 in a more efficient form
   * save many of these complications for fdml for later
   * "write" to s3?
   * consider users wanting to save work
   */
  
  /*
   * TODO: replace this with an FDML file
   * model the granule list in FDML
   * cache to spark
   * user server config to specify what to load on init
   * 
   * 'hysics" fdml
   * use dataset ref to granules id (already cached and in RDD)
   * ref as DatasetSource?
   * the rest as operations
   * 
   * define wavelength dataset in FDML
   * cache to broadcast?
   * use binary Substitution operation
   * 
   * apply iy selection before reader op
   * but not when caching the cube
   * 
   * need to "cache" RDD once we have the cube so we don't recompute it
   * need call to parallelize before ops but cache after
   * define cache=spark on both datasets
   * if data is already RddFunction, call cache on it
   *   how do we do that generically?
   *   or always call cache on RDD after set of operations
   *     CompositeOperation? instead of folding
   *       AST with binary ops...?
   *     as part of getDataset(ops) for adapter?
   * Seems like we need one dataset to do both
   *   how to specify to parallelize early?
   *   define as operation?
   */
  
  def getDataset(ops: Seq[UnaryOperation]): Dataset = {
    val xmlString = """<?xml version="1.0" encoding="UTF-8"?>
        <dataset name="hyscs_image_files"  uri="s3://hylatis-hysics-001/des_veg_cloud"  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="fdml.xsd">
            <adapter class="latis.input.HysicsGranuleListAdapter"/>
            <function>
                <scalar id="iy" type="integer"/>
                <scalar id="uri" type="text"/>
            </function>
        </dataset>
        """
    
    // Load the granule list dataset into spark
    val reader = FDMLReader(xmlString)
    //val reader = HysicsGranuleListReader() // hysics_image_files
    // iy -> uri
    val ds = reader.getDataset
 //     .unsafeForce //causes latis to use the MemoizedFunction, TODO: impl more of StreamFunction
      .restructure(RddFunction) //include this to memoize data in the form of a Spark RDD
      // only need to parallelize here
      
      /*
       * cache/force/memoize
       * if we do this (suck in all data and release source) we presumably want to reuse it
       * thus "cache" seems reasonable
       * include adding it to CacheManager
       * instruction in fdml?
       *   adapter property?
       *   operation?
       * caching is not a dataset descriptor issue
       * more of a service instance concern
       * but optimal type of cache can be dataset specific
       * 
       * also want to cache resulting "hysics" cube
       * don't rename it here
       * provide a name when caching?
       * 
       * could caching to memory work via *registering*  new dataset?
       *   the first place it looks to resolve a dataset name is the cache
       *   use backup locations if cache is expired, re-cache
       *   
       *   
       * use server config to define which datasets need to be cached on init
       *   still use fdml (adapter?), provide instruction to cache data for specific SampledFunction impl
       * also goes into CacheManager which should be first DatasetSourceProvider
       * for this we want granule list memoized to RDD first (ok to go into cache manager but not needed)
       *   then apply ops
       *   then cache RDD (not after each op as before)
       *   finally cache to CM
       * should the general caching rules be the same?
       *   go to target SampledFunction first then apply ops then cache to CM?
       *   might we ever want to apply ops first? 
       *   likely that we want to filter before memoizing
       *   use compiler... but not yet
       * Define granule list to be cached to RDD
       *   cache after loading and non-existent ops
       *   define hysics data cube starting with ref to granules
       *   likewise cache to RDD when done, since already an RDD just cache the RDD
       *   make caching to SampledFunction idempotent
       * instead of FunctionFactory.fromSeq, pass it a SF
       *   if the same type then no-op
       */
    
    //val wuri = new URI("file:/data/hysics/des_veg_cloud/wavelength.txt")
    //val defaultBase = "s3://hylatis-hysics-001/des_veg_cloud"
    //val base = LatisProperties.getOrElse("hysics.base.uri", defaultBase)
    //val wuri = new URI(s"$base/wavelength.txt")
    val wds = HysicsWavelengths() //HysicsWavelengthsReader(wuri).getDataset
    //TODO: cache to spark via broadcast?

      
    val allOps: Seq[UnaryOperation] = Seq(
      HysicsImageReaderOperation(), // Load data from each granule
      Uncurry()  // Uncurry the dataset: (iy, ix, iw) -> irradiance
    ) ++ ops
    
    // Apply Operations
    val ds2 = allOps.foldLeft(ds)((ds, op) => op(ds))
    
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

    // Rename the Dataset and add it to the LaTiS CacheManager.
    val ds4 = ds3.copy(data = data).rename("hysics")
    ds4.cache()
    ds4
  }
  
}
//  //TODO: make a Matrix subtype of SampledFunction with matrix semantics
//  //TODO: allow value to be any Variable type?
//  //TODO: impl as Adapter so we can hand it a model with metadata
//  //TODO: optimize with index logic
//  
//  val granuleListDataset: Dataset = {
//    val yType = ScalarType("iy")
//    val gType = ScalarType("uri")
//    val model = FunctionType("")(yType, gType)
//    
//    val md = Metadata("id" -> "image_files")(model)
//    
//    val base = uri.toString //"s3:/hylatis-hysics-001/des_veg_cloud"
//    val imageCount = LatisProperties.getOrElse("imageCount", "4200").toInt
//    // Use image Count to compute a stride.
//    val stride: Int = 4200 / imageCount
//    
//    val samples = Iterator.range(1, 4201, stride) map { i =>
//      val y = Integer(i)
//      val uri = Text(f"${base}/img$i%04d.txt")
//      Sample(y, uri)
//    }
//
//    Dataset(md, Function.fromSamples(samples))
//  }
//
//  // (y, x, wavelength) -> irradiance
//  val model = FunctionType("f")(
//    TupleType("")(
//      ScalarType("y"),
//      ScalarType("x"),
//      ScalarType("wavelength")
//    ),
//    ScalarType("irradiance")
//  )
//  
//  val metadata = Metadata("id" -> "hysics")(model)
//  
//  /*
//   * TODO: use adapter
//   * wavelengths: iw -> wavelength
//   * image: (ix, iw) -> f
//   * join with new dimension: index
//   *   can generic join determine this?
//   *     same types
//   *     same domain sets: either nothing to join (e.g. fill holes) or new dim - ambiguous
//   *   iy -> (ix, iw) -> f
//   * uncurry, curry,...
//   * but can't hold all in memory
//   * keep in this form
//   * convert to XY here or in spark?
//   *   sharability of geoCalc?
//   * 
//   * join wavelength
//   *   f: i -> a  join  g: i -> b
//   *   => b -> a
//   *   1st ds is about the as
//   *   match index and substitute
//   *   require special bijective function for bs?
//   *   index vs integer - no gaps
//   * vs composition
//   *   would have to flip g':  b -> i  which does require bijective
//   *   f compose g' => b -> a
//   * how might function composition work with Function Data
//   *   use eval (apply)
//   *   stream, "compose" samples
//   *   same index so we should be able to "zip" in sync
//   *   each Function subclass might want to do it differently
//   *   build into join operation (e.g. if StreamingFunction...)
//   *   or method on Function that can be overridden?
//   *   
//   * Bulk Load
//   * parallelize granule list dataset
//   * map function to build dataset in spark from url
//   * how to orchestrate?
//   *   can't just throw to spark writer?
//   *   cache perspective
//   * lazy GranuleJoin Dataset
//   *   cache the granule list in spark
//   *   no need to join, already have a single RDD
//   *   just need to map the samples to the new samples
//   * in this case we get the "iy" from the granule list dataset
//   *   iy -> uri
//   *   uri => (ix, iw) -> f
//   *   but left with nested function: iy -> (ix, iw) -> f
//   *   need to apply uncurry to RDD, no shuffling
//   *   one sample will become many samples, use flatMap
//   * keep orig dataset but cached in spark
//   *   define as lazy granule join but impl caching to parallelize uris first
//   * Dataset doesn't know if it comes from a granule join
//   *   how can "cache" do the right thing?
//   *   it is only the Data that needs to be cached - realm of adapters
//   *   but dataset doesn't even know its source
//   * Start with Dataset of URIs
//   *   define Operation to map adapter over URIs to get RDD of Data?
//   *   map Data => Sample by adding outer iy domain with Data in range
//   *   generally depends on join type
//   *   make a hysics loader for now instead of trying to make it work as a dataset cache
//   *   
//   *   
//   * Function backed by RDD?
//   * 
//   */
//  
//  private lazy val wavelengths: Array[Double] = {
//    //TODO: make wavelength dataset
//    val wuri = URI.create(s"${uri.toString}/wavelength.txt")
//    val is = URIResolver.resolve(wuri).getStream
//    val source = Source.fromInputStream(is)
//    val data = source.getLines().next.split(",").map(_.toDouble)
//    source.close
//    data
//  }
//
//  
//  /**
//   * Read slit images (x,w) -> f
//   * TODO: add lon, lat
//   * then join on a new axis "y"
//   *   iy -> (ix, iw) -> f
//   * transform into x/y space and add wavelengths
//   * uncurry but don't transpose (order implications)
//   *   (y, x, w) -> f
//   */
//  def getDataset(operations: Seq[Operation]): Dataset = {
//    val samples: Iterator[Sample] = granuleListDataset.samples.zipWithIndex flatMap {
//      case (Sample(_, Seq(_, Text(uri))), iy) =>
//        MatrixTextReader(new URI(uri)).getDataset().samples map {
//          case Sample(_, Seq(Index(ix), Index(iw), Text(v))) =>
//            val (x, y) = HysicsUtils.indexToXY((ix, iy))
//            Sample(3, Real(y), Real(x), Real(wavelengths(iw)), Real(v.toDouble))
//        }
//    }
//
//    val dataset = Dataset(metadata, Function.fromSamples(samples))
//    operations.foldLeft(dataset)((ds, op) => op(ds))
//  }
//}
//
//object HysicsReader {
//  
//  def apply() = {
//    val defaultURI = "s3://hylatis-hysics-001/des_veg_cloud"
//    val uri = LatisProperties.getOrElse("hysics.base.uri", defaultURI)
//    new HysicsReader(URI.create(uri))
//  }
//
//}
