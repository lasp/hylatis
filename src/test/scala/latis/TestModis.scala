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
     *   **      *maybe for resampling onto grid with nearest neighbor: put points on grid and replace if a closer one comes along
     *       need to restructure, Array2D? Indexed? Other optimized structure? nested Maps? Tree?
     *         
     * 
     * brute force: look at every pixel
     * could take advantage or ordering
     * evaluating with a grid might lend itself to streaming or at least optimized use of ordering
     * 
     * approx swath with Cartesian x-y CS
     *   close to ix and iy values
     *   computable from (lon, lat) => (x, y)
     *   just a rotation if path is linear
     *   
     * 
     * TODO: plot distribution of points, how regular?
     * 
     * 
     * * Resample (nearest neighbor) onto regular grid
     * nearest neighbor stream resampler
     * bucket for each target point
     * use simple math to determine which bucket each data point goes into
     *   use interp on values in the bucket
     *   keep the closest in each bin for nearest neighbor, minimizes memory use
     *   given some ordering, some buckets would likely stop getting filled and could be "aggregated" early
     * SF.apply(set) could use implicit resampler
     *   while apply(point) uses interpolation
     *   resampler could be built from an interpolation?
     * Resampler:
     *   SF.apply(DomainSet)(implicit r: Resampler): SF
     *   make builder for new SF with new domain set
     *     as dataset? (x, y) -> i -> sample
     *       i -> sample could simply be a SF
     *       (x, y) -> (x', y') -> a
     *       then apply an Op that reduces the SF to a single sample 
     *         e.g. use x,y and find the sample with the nearest x',y'
     *         effectively apply interp to the x',y' sub grid!
     *     or sorted Map?
     *     use our nested Map? then wrap it as a SF? CartesianMap2D in latis3-beta
     *     
     * TODO: make sure cell is centered on x,y
     * 
     * make SF from DomainSet and range seq
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
    
    // Define regular grid to resample onto
    val (nx, ny) = (3,4)
    val domainSet = LinearSet2D(1, 2.0, nx, 1, 2.0, ny)
    //domainSet.elements foreach println

    val cells: Array[Buffer[Sample]] = 
      Array.fill(domainSet.length)(Buffer.empty)
    
    origSF.samples foreach {
      case sample @ Sample(domain, _) =>
        // (x, y) looking for a home in the regular grid (x2, y2) 
        // Put sample into cells array by index of the target grid
        val index = domainSet.indexOf(domain)
        if (index >= 0 && index < domainSet.length) cells(index) += sample
        //else out of bounds so ignore
    }
    //cells foreach println
    
    // Define function to compute the distance between two DomainData
    val distance = (dd1: DomainData, dd2: DomainData) => {
      //TODO: assert same length
      //TODO: support any Numeric
      val squares = (dd1 zip dd2) map {
        case (d1: Double, d2: Double) => Math.pow((d2 - d1), 2)
      }
      Math.sqrt(squares.sum)
    }
    
    // Make a new SF with the samples in each cell then evaluate it
    // with the domain value of that cell to get the final range value.
    val range: Seq[RangeData] = (domainSet.elements zip cells) map { 
      case (domain, samples) =>
        //TODO: use SF that is more suitable for interp
        //val f = SeqFunction(samples) //TODO: specify interp strategy
        //f(data) getOrElse ??? //TODO: fill data
        
        // Do interp here until SF has it
        /*
         * Interpolation
         * need access to all the samples of the SF
         * awkward to construct implicitly? generally want to be explicit anyway or via config
         * *should SF extend/mixin an interpolation?
         * may need to explore combinations of SF structure and interp type
         * simply built in to the SF.apply? that is effectively the interp API
         * or separate interp? function=?
         * 
         */
        // Nearest neighbor
        val nearestSample = samples minBy {
          case Sample(dd, _) => distance(domain, dd)
        }
        //val newSample = Sample(domain, nearestSample.range)
        nearestSample.range
    }
    
    // Build new Dataset
    //Note: could build samples above, but this way we preserve the topology of the regular grid
    val data = SetFunction(domainSet, range)

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
