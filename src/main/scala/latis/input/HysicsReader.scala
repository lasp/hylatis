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

/**
 * Read the Hysics granule list dataset, cache it into Spark,
 * and apply operation to read and structure the data.
 * Cache the LaTiS Dataset in memory so we don't have to reload 
 * it into spark each time.
 */
case class HysicsReader() extends DatasetSource {
  
  def getDataset(ops: Seq[UnaryOperation]): Dataset = {
    // Load the granule list dataset into spark
    val reader = HysicsGranuleListReader() // hysics_image_files
    // iy -> uri
    val ds = reader.getDataset().copy(metadata = Metadata("hysics"))
 //     .unsafeForce //causes latis to use the MemoizedFunction, TODO: impl more of StreamFunction
      .cache(RddFunction) //include this to memoize data in the form of a Spark RDD
    
    /*
     * TODO: read wavelength dataset here (now in HysicsImageReaderOperation) and substitute
     * what about order implications since iw is a domain variable
     */
    val wuri = new URI("file:/data/hysics/des_veg_cloud/wavelength.txt")
    val wds = HysicsWavelengthsReader(wuri).getDataset(Seq.empty)
    //TODO: cache to spark via broadcast?
      
    /*
     * TODO:
     * define granule list dataset (fdml?)
     * optional property: cache="rdd"
     * encode these ops
     */
      
    val allOps: Seq[UnaryOperation] = Seq(
      HysicsImageReaderOperation(), // Load data from each granule
      Uncurry(),  // Uncurry the dataset: (iy, ix, iw) -> irradiance
      Substitution2(wds) //replace wavelength index with wavelength value
    ) ++ ops
    
    // Apply Operations
    val ds2 = allOps.foldLeft(ds)((ds, op) => op(ds))
    
    //cache in memory
    CacheManager.cacheDataset(ds2)
    
    ds2
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
