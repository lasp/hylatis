package latis.input

import java.net.URL
import scala.io.Source
import latis.ops._
import latis.data._
import latis.metadata._
import scala.collection.mutable.ArrayBuffer
import latis.Dataset
import latis.util.HysicsUtils
import latis.util.AWSUtils
import java.net.URI

class HysicsLocalReader(uri: URI) extends DatasetSource {
  //TODO: make a Matrix subtype of SampledFunction with matrix semantics
  //TODO: allow value to be any Variable type?
  //TODO: impl as Adapter so we can hand it a model with metadata
  //TODO: optimize with index logic
  
  val granuleListDataset: Dataset = {
    val yType = ScalarType("iy")
    val gType = ScalarType("uri")
    val model = FunctionType("")(yType, gType)
    
    val md = Metadata("id" -> "image_files")(model)
    
    val base = uri.toString //"s3:/hylatis-hysics-001/des_veg_cloud"
 //   val samples = Iterator.range(1, 4201) map { i =>
val samples = Iterator.range(1, 51) map { i =>
      val y = Integer(i)
      val uri = Text(f"${base}/img$i%04d.txt")
      Sample(y, uri)
    }
    
    Dataset(md, Function.fromSamples(samples))
  }

  // (y, x, wavelength) -> irradiance
  val model = FunctionType("f")(
    TupleType("")(
      ScalarType("y"),
      ScalarType("x"),
      ScalarType("wavelength")
    ),
    ScalarType("irradiance")
  )
  
  val metadata = Metadata("id" -> "hysics")(model)
  
  /*
   * TODO: use adapter
   * wavelengths: iw -> wavelength
   * image: (ix, iw) -> f
   * join with new dimension: index
   *   can generic join determine this?
   *     same types
   *     same domain sets: either nothing to join (e.g. fill holes) or new dim - ambiguous
   *   iy -> (ix, iw) -> f
   * uncurry, curry,...
   * but can't hold all in memory
   * keep in this form
   * convert to XY here or in spark?
   *   sharability of geoCalc?
   * 
   * join wavelength
   *   f: i -> a  join  g: i -> b
   *   => b -> a
   *   1st ds is about the as
   *   match index and substitute
   *   require special bijective function for bs?
   *   index vs integer - no gaps
   * vs composition
   *   would have to flip g':  b -> i  which does require bijective
   *   f compose g' => b -> a
   * how might function composition work with Function Data
   *   use eval (apply)
   *   stream, "compose" samples
   *   same index so we should be able to "zip" in sync
   *   each Function subclass might want to do it differently
   *   build into join operation (e.g. if StreamingFunction...)
   *   or method on Function that can be overridden?
   *   
   */
  
  private lazy val wavelengths: Array[Double] = {
    //TODO: make wavelength dataset
    val wuri = URI.create(s"${uri.toString}/wavelength.txt")
    val is = URIResolver.resolve(wuri).getStream
    val source = Source.fromInputStream(is)
    val data = source.getLines().next.split(",").map(_.toDouble)
    source.close
    data
  }

  
  /**
   * Read slit images (x,w) -> f
   * TODO: add lon, lat
   * then join on a new axis "y"
   *   iy -> (ix, iw) -> f
   * transform into x/y space and add wavelengths
   * uncurry but don't transpose (order implications)
   *   (y, x, w) -> f
   */
  def getDataset(operations: Seq[Operation]): Dataset = {
val xops = List(Select("row < 50"))
    val samples: Iterator[Sample] = granuleListDataset.samples.zipWithIndex flatMap {
      case (Sample(_, Seq(_, Text(uri))), iy) => 
        MatrixTextReader(new URI(uri)).getDataset(xops).samples map {
          case Sample(_, Seq(Index(ix), Index(iw), Text(v))) =>
            val (x, y) = HysicsUtils.indexToXY((ix, iy))
            Sample(3, Real(y), Real(x), Real(wavelengths(iw)), Real(v.toDouble))
        }
    }
    
//    val samples: Iterator[Sample] = uris.zipWithIndex.flatMap { case (uri, iy) =>
//      val image: Dataset = MatrixTextReader(uri).getDataset()
//
//      val nx = image.length
//      val nw = wavelengths.length
//
//      for (
//        ix <- Iterator.range(0, nx);
//        iw <- Iterator.range(0, nw)
//      ) yield {
//        //val ll = HysicsUtils.indexToGeo(ix, iy)
//        val ll = HysicsUtils.indexToXY(ix, iy) //leave in native Cartesian x/y space
//        val lon = Scalar(ll._1)
//        val lat = Scalar(ll._2)
//        val w = Scalar(wavelengths(iw))
//        val value = Scalar(image(ix)(iw).toDouble)
//        val domain = Tuple(lon, lat, w)
//        Sample(domain, value)
//      }
//    }

    val dataset = Dataset(metadata, Function.fromSamples(samples))
    operations.foldLeft(dataset)((ds, op) => op(ds))
  }
}

object HysicsLocalReader {
  
  //def apply() = new HysicsLocalReader("/data/hysics/des_veg_cloud/")
  def apply() = new HysicsLocalReader(URI.create("s3:/hylatis-hysics-001/des_veg_cloud"))

  def apply(s3Base: String): HysicsLocalReader = {
    new HysicsLocalReader(URI.create(s3Base))
  }
}