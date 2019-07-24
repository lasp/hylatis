package latis

import latis.data._
import latis.input.ModisReader
import latis.model._
import latis.ops.RGBImagePivot
import latis.output.ImageWriter

import org.junit._
import scala.collection.mutable.SortedMap
import latis.util.CoordinateSystemTransform
import latis.metadata.Metadata
import latis.output.TextWriter
import scala.collection.mutable.Buffer
import latis.util.StreamUtils
import cats.effect.IO
import latis.data.SetFunction
import latis.resample.BinResampling

class TestModis {
  
  //@Test
  def reader() = {
    //Note: we can override the config:
    //System.setProperty("hylatis.modis.uri", "/data/modis/MYD021KM.A2014230.2120.061.2018054180853.hdf")
    val ds = ModisReader().getDataset
    //TextWriter(System.out).write(ds)
    val image = RGBImagePivot("band", 1.0, 4.0, 2.0)(ds)
    //TextWriter(System.out).write(image)
    ImageWriter("/data/modis/modisRGB.png").write(image)
  }
  
  @Test
  def interpolation() = {
    /*
     * Given (longitude, latitude) provide (ix, iy)
     * modis: band -> (ix, iy) -> radiance
     * 
     * We have nc vars to get Dataset: (ix, iy) -> (longitude, latitude)
     * need to get *inverse* (not unlike ML problem?)
     * invert with interpolation strategy? 
     *   is that generalizable? (e.g. linear)
     *   need specific interp function? (e.g. ModisUtils)
     *   will call apply(lon, lat) on SF
     *     do we need a special SF, hope not
     *     can SF.apply take an implicit Interpolation type class?
     *       can't likely support stream
     *      *maybe for resampling onto grid with nearest neighbor: put points on grid and replace if a closer one comes along
     *       need to restructure, Array2D? Indexed? Other optimized structure? nested Maps? Tree?
     * 
     * brute force: look at every pixel, slow ~ O(n*m)
     * could take advantage of ordering
     * evaluating with a grid might lend itself to streaming or at least optimized use of ordering
     * 
     * approx swath with Cartesian x-y CS
     *   close to ix and iy values
     *   computable from (lon, lat) => (x, y)
     *   just a rotation if path is linear
     *   plot distribution of points, how regular?
     * 
     * 
     * * Resample (nearest neighbor) onto regular grid
     * nearest neighbor stream resampler
     * bucket for each target point
     * use simple math to determine which bucket each data point goes into
     *   use interp on values in the bucket
     *   keep the closest in each bin for nearest neighbor, minimizes memory use
     *   given some ordering, some buckets would likely stop getting filled and could be "aggregated" early
     * SF.resample(set) could use resampler
     *   while apply(point) uses interpolation
     *   resampler could be built from an interpolation?
     * Resampler:
     *   SF.apply(DomainSet)(r: Resampler): SF
     *   make builder for new SF with new domain set
     *     as dataset? (x, y) -> i -> sample
     *       i -> sample could simply be a SF
     *       (x, y) -> (x', y') -> a
     *       then apply an Op that reduces the SF to a single sample 
     *         e.g. use x,y and find the sample with the nearest x',y'
     *         effectively apply interp to the x',y' sub grid!
     *     or sorted Map?
     *     use our nested Map? then wrap it as a SF? CartesianMap2D in latis3-beta
     *     within the context of the resampler, might as well deal with Seq[Sample] instead of nested SF
     *     
     */
    
    // Define original Function
    val (onx, ony) = (50, 50)
    val set = LinearSet2D(0.2, 1.0, onx, 0.2, 1.0, ony)
    val values = for {
      ix <- (0 until onx)
      iy <- (0 until ony)
    } yield RangeData(1.0 * ix * iy)
    val origSF = SetFunction(set, values)
    //origSF.samples foreach println
    /*
     * TODO: use actual modis data here
     * need to substitute lon-lat values
     * substitute:
     *   (ix, iy) -> f  "join"  (ix, iy) -> (lon, lat)
     *   => (lon, lat) -> f
     * Substitution Op assume 1D
     * 
     * could we use CSX?
     * e.g. when putting into cells
     * 
     * or just stitch it in in the adapter?
     */
    
    // Define regular grid to resample onto
    val (nx, ny) = (3,4)
    val domainSet = LinearSet2D(1, 2.0, nx, 1, 2.0, ny)
    //domainSet.elements foreach println
      
    
    //val newSF = origSF.resample(domainSet, BinResampling())
    
    
    
    
//  case class BinResampling() {
//    
//    /*
//     * Define function to resample the original grid onto the regular domain set.
//     * SF.resample should use this encapsulated as BinningResampler?
//     *   Note, cells use index from DomainSet so make no assumption about suitability
//     * TODO: support Stream
//     */
//    def resample(sf: SampledFunction, domainSet: DomainSet): SampledFunction = {
//
//      // Define Array of Buffers to accumulate Samples
//      // into cells of the target DomainSet.
//      val cells: Array[Buffer[Sample]] =
//        Array.fill(domainSet.length)(Buffer.empty)
//
//      // Traverse the Samples of the original SampledFunction
//      // and put them into cells of the target DomainSet
//      // based on their domain values.
//      //TODO: streamSamples
//      def addSampleToCells(sample: Sample): Unit = {
//        val index = domainSet.indexOf(sample.domain)
//        if (index >= 0 && index < domainSet.length) cells(index) += sample
//        //else out of bounds so ignore
//      }
//    
//      // Consume the samples from the original SF and put them in the cells of the new DomainSet
//      // Note that Stream does not have foreach but we can ignore the return values with drain.
//      sf.streamSamples.map(addSampleToCells).compile.drain.unsafeRunSync
//
//      // Compute the new range values by finding the sample
//      // in each cell that is closest to the center.
//      val range: Seq[RangeData] = (domainSet.elements zip cells) map {
//        case (domain, samples) =>
//          //TODO: use SF that is more suitable for interp
//          //val f = SeqFunction(samples) //TODO: specify interp strategy
//          //f(data) getOrElse ??? //TODO: fill data
//
//          // Do interp here until SF has it
//          /*
//         * Interpolation
//         * need access to all the samples of the SF
//         * awkward to construct implicitly? generally want to be explicit anyway or via config
//         * *should SF extend/mixin an interpolation?
//         * may need to explore combinations of SF structure and interp type
//         * simply built in to the SF.apply? that is effectively the interp API
//         * or separate interp? function=?
//         * 
//         *
//         * interp: Seq[Sample] => Sample then eval with DomainData ?
//         * but equiv to SF => Sample which could have optimized SF
//         * chicken and egg, back to evaluating a SF
//         * sounds like apply should have interp built in, via mixin
//         * could some classes of SF always use a given interp that it is optimized for?
//         * restructure to that SF to get the interp you want?
//         * But many interp algorithms just need n nearest samples (like the cell approach provides)
//         *   maybe special SFs can do that part differently then use same interp algorithm
//         *   using that subset of Samples as a Seq
//         * *return sample vs range?
//         * 
//         * NearestNeighbor as trait with MemoizedFunction self type?
//         *   or SampledFunction with EvaluatableOnce?
//         *   defines interpolate(dd: DomainData): Option[Sample] = use samples
//         *   SF.apply(dd) = interpolate(dd).map.(_.range)
//         * hard to mixin dynamically?
//         *   InterpFactory? build from samples? or SF itself?
//         *   some pairs of SF and Interp can be optimized
//         *     in interp code with cases for SF or otherway around
//         *     SF is more likely to be extended
//         *    *put optimization for interp types in SF
//         *     e.g. finding subset of samples to pass to interp instead of all samples
//         *     interp match NN => override NN behavior
//         *   Need partial construction (e.g. factory)
//         *     built by SF with (subset of) samples
//         *     the interp could construct clever reusable structure
//         *     then interp(dd)
//         *     NearestNeighbor(samples)(dd)   
//         *     
//         * Should interp only be called if matching sample not found?
//         * getting existing sample is generally something special SFs can do
//         *   e.g. CartesianMaps, Arrays
//         *   if not found then it can delegate to interp
//         *   can't do for general Stream
//         *   does SF need to be Interpolatable?
//         *   
//         * What about Extrapolation
//         *   if NN is used for each cell then we need it
//         *   up to SF to determine if it's out of bounds then use extrap?
//         *   NN as both Interp and Extrap
//         *  *SF.apply should have args for both 
//         */
//          // Nearest neighbor
//          val nearestSample = samples minBy {
//            case Sample(dd, _) => DomainData.distance(domain, dd)
//          }
//          //val newSample = Sample(domain, nearestSample.range)
//          nearestSample.range
//      }
//
//      // Build new Dataset
//      //Note: could build samples above, but this way we preserve the topology of the regular grid
//      SetFunction(domainSet, range)
//
//    }
//  }
    
    /*
     * TODO: define SF.resample with Resampling
     * how can we get inter/extrap for general use?
     * 
     * 
     */
    val data = BinResampling().resample(origSF, domainSet)
    
    val model = Function(
      Tuple(
        Scalar(Metadata("id" -> "x", "type" -> "double")),
        Scalar(Metadata("id" -> "y", "type" -> "double"))
      ),
//      Function(
//        Tuple(
//          Scalar(Metadata("id" -> "x0", "type" -> "double")),
//          Scalar(Metadata("id" -> "y0", "type" -> "double"))
//        ),
        Scalar(Metadata("id" -> "v", "type" -> "double"))
//      )
    )
    
    val md = Metadata("modis_test")
    
    val ds = Dataset(md, model, data)
    TextWriter().write(ds)
  }
}
