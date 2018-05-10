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

class HysicsLocalReader(dir: String) extends DatasetSource {
  //TODO: make a Matrix subtype of SampledFunction with matrix semantics
  //TODO: allow value to be any Variable type?
  //TODO: impl as Adapter so we can hand it a model with metadata
  //TODO: optimize with index logic
  
  val granuleListDataset: Dataset = {
    val yType = ScalarType("y")
    val gType = ScalarType("uri")
    val model = FunctionType("")(yType, gType)
    
    val md = Metadata("id" -> "image_files")(model)
    
    val base = "s3:/hylatis-hysics-001/des_veg_cloud"
 //   val samples = Iterator.range(1, 4201) map { i =>
    val samples = Iterator.range(1, 11) map { i =>
      val y = Integer(i)
      val uri = Text(f"${base}/img$i%04d.txt")
      Sample(y, uri)
    }
    
    Dataset(md, Function.fromSamples(samples))
  }
  
  val delimiter = "," //TODO: parameterize

  val model = {
    val xType = ScalarType("x")
    val yType = ScalarType("y")
    val wavelength = ScalarType("wavelength")
    val domain = TupleType("")(xType, yType, wavelength)
    val range = ScalarType("value")
    FunctionType("f")(domain, range)
  }
  
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
    val source = Source.fromFile(dir + "wavelength.txt")
    val data = source.getLines().next.split(",").map(_.toDouble)
    source.close
    data
  }
   
  private def readImage(uri: URI): Array[Array[Double]] = {
    val path = uri.getPath.drop(1)  // drop leading "/"
    // Split bucker name from object name at first "/".
    val (bucket, file) = path.splitAt(path.indexOf('/')) match {
      case (b, f) => (b, f.drop(1))
    }
    
    val s3 = AWSUtils.s3Client.get
    val is = s3.getObject(bucket, file).getObjectContent
    val source = Source.fromInputStream(is)
    val data = source.getLines.map(_.split(delimiter).map(_.toDouble)).toArray
    source.close
    data
  }
  
  /**
   * Read slit images (x,w) -> f
   * TODO: add lon, lat
   * then logically join on a new axis "y"
   */
  def getDataset(operations: Seq[Operation]): Dataset = {
    val uris = granuleListDataset.samples map {
      case Sample(_, Seq(_, Text(uri))) => new URI(uri)
    }
    
    val samples: Iterator[Sample] = uris.zipWithIndex.flatMap { case (uri, iy) =>
      val image: Array[Array[Double]] = readImage(uri)

      val nx = image.length
      val nw = wavelengths.length

      for (
        ix <- Iterator.range(0, nx);
        iw <- Iterator.range(0, nw)
      ) yield {
        //val ll = HysicsUtils.indexToGeo(ix, iy)
        val ll = HysicsUtils.indexToXY(ix, iy) //leave in native Cartesian x/y space
        val lon = Scalar(ll._1)
        val lat = Scalar(ll._2)
        val w = Scalar(wavelengths(iw))
        val value = Scalar(image(ix)(iw).toDouble)
        val domain = Tuple(lon, lat, w)
        Sample(domain, value)
      }
    }

    val dataset = Dataset(metadata, Function.fromSamples(samples))
    operations.foldLeft(dataset)((ds, op) => op(ds))
  }
}

object HysicsLocalReader {
  
  def apply() = new HysicsLocalReader("/data/hysics/des_veg_cloud/")

  def apply(dir: String): HysicsLocalReader = {
    new HysicsLocalReader(dir)
  }
}