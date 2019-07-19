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
    val xs: Array[Any] = Array.tabulate(50)((i: Int) => 0.2 * i)
    val ys: Array[Any] = Array.tabulate(50)((i: Int) => 0.2 * i)
    val vs: Array[Array[Any]] = Array.tabulate(100, 100)((i: Int, j: Int) => 1.0 * i * j) 
    val origSF = IndexedFunction2D(xs, ys, vs)
    //origSF.samples foreach println
    
    //Define CSR for new grid, transform to/from index space
    /*
     * TODO: generalize CSXs, property tests
     * CSX(CSR1, CSR2)
     *   csx. to, from?
     * just a bijective function
     *   CSX: p1 <=> p2, d is a point in the domain, DomainData
     *     2D: (x1, y1) <=> (x2, y2)
     *     apply
     *     inverse.apply
     * can we construct a CSX given 2 CSRs? 
     *   what would a CSR need?
     *   only valid if all use same space? e.g. lon,lat
     *   don't see how
     *   could combine 2 CSXs if they share a CS space
     * we probably just need to think in terms of bijective function
     *   have to, from functions as vals
     *   inverse makes a new one with to,from swapped? 
     */
    val csx = CoordinateSystemTransform(
      // Forward transform
      (data: DomainData) => data match {
        case DomainData(ix: Int, iy: Int) =>
          DomainData(1.0 * (ix + 1), 2.0 * (iy + 1))
        case _ => ??? //TODO: error, invalid data
      },
      // Reverse transform: (x, y) => (ix, iy)
 //TODO: round such that we are cell-centered
      (data: DomainData) => data match {
        case DomainData(x: Double, y: Double) =>
          DomainData(x.toInt - 1, (y/2.0).toInt - 1)
        case _ => ??? //TODO: error, invalid data
      }
    )
    
    // Define regular grid to resample onto
    val (nx, ny) = (3,4)
    val domainSet: DomainSet = DomainSet(
      for {
        ix <- 0 until nx
        iy <- 0 until ny
      } yield csx(DomainData(ix, iy))
    )
    //domainSet.elements foreach println
    
    // Map (x, y) points of target (regular) grid to the Seq of Samples that fall in that cell
    //val cells = SortedMap[(Double,Double), Seq[Sample]]() 
    val cells = SortedMap[DomainData, Seq[Sample]]() 
    /*
     * this is effectively a SF, but need to be able to mutate, build
     * use CartesianMap2D?, wrap as SF
     */
    origSF.samples foreach {
      /*
       * Compute which new cell to put this sample in
       *   could be multiple to support richer interp
       * Use inverse of domain set generation math (CSX) to get index
       * Note: this origSF needs to have same domain type as target
       *   TODO: apply CSX to go from native to target, e.g. modis index to lon,lat
       *   
       * should we keep the cells in index space?
       * could use 2D array
       * but so close to being the SF we want
       * Ints are smaller
       */
      case sample @ Sample(domain, _) =>
        // (x, y) looking for a home in the regular grid (x2, y2) 
        // 
        //indices of the target cell = key into cells Map
        val d2 = csx.inverse(domain)
        val (ix, iy) = d2 match {
          case DomainData(ix: Int, iy: Int) => (ix, iy)
        }
        
        // Add the orig Sample into the appropriate cell of the target domain set
        val z = cells.get(d2) 
        z match {
          case Some(s) => cells += d2 -> (s :+ sample)
          case None => 
            if (ix >= 0 && ix < nx && iy >= 0 && iy < ny) 
              cells += d2 -> Seq(sample)
            //else sample outside desired grid
        }
    }
    //cells foreach println
    
    // Make new SF from cells Map
    val samples = cells.toSeq map {
      case (domain, samples) =>
        Sample(csx(domain), RangeData(SeqFunction(samples)))
    }
    val data = SeqFunction(samples)
    
    val model = Function(
      Tuple(
        Scalar(Metadata("id" -> "x", "type" -> "double")),
        Scalar(Metadata("id" -> "y", "type" -> "double"))
      ),
      Function(
        Tuple(
          Scalar(Metadata("id" -> "x0", "type" -> "double")),
          Scalar(Metadata("id" -> "y0", "type" -> "double"))
        ),
        Scalar(Metadata("id" -> "v", "type" -> "double"))
      )
    )
    
    val md = Metadata("modis_test")
    
    val ds = Dataset(md, model, data)
    TextWriter().write(ds)
  }
}
