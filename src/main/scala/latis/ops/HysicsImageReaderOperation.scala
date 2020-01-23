package latis.ops

import latis.data._
import latis.metadata._
import latis.model._
import java.net.URI

import latis.input.HysicsImageReader
import latis.util.LatisException

/**
 * Operation on a granule list dataset to get data for each URI.
 * Given a granule list dataset:
 *   ix -> uri
 * and an inage reader:
 *   uri => (iy, iw) -> radiance
 * generate the dataset:
 *   ix -> (iy, iw) -> radiance
 */
case class HysicsImageReaderOperation() extends MapRangeOperation {

  def mapFunction(model: DataType): TupleData => TupleData = {
    // Get the position of the uri variable
    val pos: Int = model.getPath("uri") match {
      case Some(RangePosition(i) :: Nil) => i
      case _ =>
        val msg = "Failed to find 'uri' in the input Dataset."
        throw LatisException(msg)
    }

    (range: TupleData) => range.elements(pos) match {
      case Text(uri) =>
        // (iy, iw) -> radiance
        val image: MemoizedFunction = HysicsImageReader.read(new URI(uri)).unsafeForce().data
        TupleData(List(image))
      case _ => throw LatisException("uri is not defined as text")
    }
  }

  // ix -> (iy, iw) -> radiance
  override def applyToModel(model: DataType): DataType = model match {
    case Function(domain, _) =>
      Function(
        domain,
        HysicsImageReader.model
      )
    case _ => throw LatisException(s"Invalid dataset: $model")
  }

  //  def wavelengths: Array[Double] = {
  //    val defaultURI = "s3://hylatis-hysics-001/des_veg_cloud"
  //    val uri = new URI(LatisProperties.getOrElse("hysics.base.uri", defaultURI))
  //    val wuri = URI.create(s"${uri.toString}/wavelength.txt")
  //    //TODO: fs2 Stream: val is = NetUtils.resolve(wuri).getStream
  //    val is = wuri.toURL.openStream
  //    val source = Source.fromInputStream(is)
  //    val data = source.getLines().next.split(",").map(_.toDouble)
  //    source.close
  //    data
  //  }
  //
  //  /**
  //   * Read the array of wavelength values from the data file
  //   * and broadcast for reuse.
  //   */
  //  private def broadcastWavelengths() =
  //    SparkUtils.sparkContext.broadcast(wavelengths) //TODO: add util?


  //  /**
//   * Construct a function to convert samples of URIs to samples of image data
//   * used to make the hysics data cube: ix -> (iy, wavelength) -> radiance
//   */
//  def makeMapFunction(model: DataType): Sample => Sample = {
//    //Read and broadcast the wavelength values
////    val bcWavelengths = broadcastWavelengths()
//
//    //Define function to map URIs to data samples
//    (sample: Sample) => sample match {
//      // Sample of granule list: ix -> URI
//      //TODO: use model to determine sample value for URI
//      //  assume uri is first in range for now
//      //TODO: enforce by projecting only "uri"?
//      case Sample(domain, RangeData(Text(uri))) =>
// //       val ws = bcWavelengths.value
// //val ws = wavelengths
//        val image = HysicsImageReader().read(new URI(uri)) // (iy, iw) -> radiance
//
//        /*
//         * TODO: use a join to replace iw with w
//         * iw -> f  <join>  iw -> w  =>  w -> f
//         * regular join: iw -> (f, w)
//         *   could then groupBy(w)
//         * Compose?
//         *   reverse bijective Function: w -> iw
//         *   iw -> f  <compose>  w -> iw  =>  w -> f
//         *   trickier when embedded in more complex Function
//         *     need to curry
//         *     apply to nested function
//         * Substitute?
//         *   via evaluation
//         *   seems easier to implement
//         *   not quite the right semantics?
//         *     "substitute/replace iw with w"
//         * could leave data in index coords and use
//         *
//         *
//         * Broadcast as cache
//         * read wavelength dataset
//         * cache="broadcast"
//         * BroadcastFunction(samples: Seq[Sample]) extends MemoizedFunction
//         * does it help?
//         *
//         */
////        //replace iw with wavelength values: (ix, wavelength) -> radiance
//////TODO: use operation, update model
////        val sf = image.data map {
////          case Sample(DomainData(ix, iw: Int), range) => Sample(DomainData(ix, ws(iw)), range)
//////TODO: need to sort samples; ws are descending
////        }
//
//        Sample(domain, RangeData(image.data))
//    }
//  }
//
//  override def applyToData(data: SampledFunction, model: DataType): SampledFunction =
//    data.map(makeMapFunction(model))
  

    
}
