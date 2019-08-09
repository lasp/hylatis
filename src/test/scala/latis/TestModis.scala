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
import latis.data.LinearSet2D
import java.io.FileOutputStream
import latis.input.ModisGeolocationReader
import latis.ops.Substitution
import latis.ops._

class TestModis {
  
  //@Test
  def reader() = {
    //Note: we can override the config:
    //System.setProperty("hylatis.modis.uri", "/data/modis/MYD021KM.A2014230.2120.061.2018054180853.hdf")
    val ds = ModisReader().getDataset
    //TextWriter(System.out).write(ds)
    val image = RGBImagePivot("band", 1.0, 1.0, 1.0)(ds)
    //TextWriter(System.out).write(image)
    ImageWriter("/data/modis/modisRGB.png").write(image)
    /*
     * TODO: problem with consistent types
     * band values were stored at floats in the RDD
     * RGBImagePivot evaluates it with doubles
     *   we must have gotten lucky before, 1.0d==1.0f but 1.1d!=1.1f
     * would be a shame to store data as doubles in the rdd if we don't have to
     * this is a general concern for Function evaluation
     * do we just need to do all data as doubles (and longs)?
     * can we use metadata? SF knows nothing about metadata
     * 
     */
  }
  
  lazy val geoLocation = {
    val gds = ModisGeolocationReader().getDataset // (ix, iy) -> (lon, lat), NOT Cartesian
    gds.restructure(ArrayFunction2D) //reading geoloc data into 2D array ~ 10s
  }

  lazy val toyGeoLocation = {
//    val samples = for {
//      ix <- (0 until 203)
//      iy <- (0 until 136)
//      lon: Double = ix / 10.0
//      lat: Double = iy / 10.0
//    } yield Sample(DomainData(ix*10, iy*10), RangeData(lon, lat))
//    val data = SampledFunction.fromSeq(samples)
    val array: Array[Array[RangeData]] = Array.tabulate(2030, 1354)((i,j) => RangeData(i/10.0, j/10.0))
    val data = ArrayFunction2D(array)
    
    val model = Function(
      Tuple(Scalar("ix"), Scalar("iy")),
      Tuple(
        Scalar(Metadata(
          "id" -> "longitude",
          "type" -> "float",
          "origName" -> "MODIS_Swath_Type_GEO/Geolocation_Fields/Longitude")),
        Scalar(Metadata(
          "id" -> "latitude",
          "type" -> "float",
          "origName" -> "MODIS_Swath_Type_GEO/Geolocation_Fields/Latitude"))))
          
    Dataset(Metadata(""), model, data) 
  }
  
  /*
   * TODO: load into Spark
   * we ultimately want w -> (x, y) -> f
   * we now read the 3D grid the curry before putting into spark
   *   ok but possible memory limits
   * could load a smaller dataset then read the bands in parallel, akin to goes?
   *   define a dataset for each band using netcdf slice on 1st dimension
   */
  
  //@Test
  def resample() = {
    /*
     * Given (longitude, latitude) provide (ix, iy)
     * modis: band -> (ix, iy) -> radiance
     * 
     * We have nc vars to get Dataset: (ix, iy) -> (longitude, latitude)
     * define this dataset from MYD03 file: "MODIS/Aqua Geolocation Fields 5-Min L1A Swath 1km"
     * TODO: use compressed lon-lat in orig data file
     * join via substitution
     *   need to be able to substiture a tuple
     *   tuple needs id
     * 
     * Could we apply CSX index -> geo in the process of binning?
     * akin to implicit conversion
     *   we have A but need B and we have implicit function A => B
     * special ModisBinResampling? seems like overkill
     * we need to do the substitution somewhere
     * would rather keep data on Cartesian grid even if it is indices
     * but doesn't really matter for now?
     *   just a diff example of how we can manage data 
     *   but couldn't make image in orig geo coords
     */
    
    val ds0 = ModisReader().getDataset
    //TextWriter(System.out).write(ds0)
    /*
     * stride = 10:
     *   read: 4.3s
     *   write: 20s
     * stride = 1:
     *   read: too slow
     */

    // band -> (longitude, latitude) -> radiance
    val ds = Substitution()(ds0, geoLocation) // < 1s, 10s to read geoloc; 20s in spark with 2 bands, not parallel
//val ds = Substitution()(ds0, toyGeoLocation)
   //TextWriter(System.out).write(ds)
    
    // Define regular grid to resample onto
    val (nx, ny) = (30, 25)
    val domainSet = LinearSet2D(1, -110, nx, 1, 10, ny)
    //val domainSet = LinearSet2D(0.5, -110, nx, 0.5, 10, ny)
//for toy grid
//val (nx, ny) = (200,130)
//val domainSet = LinearSet2D(1, 0, nx, 1, 0, ny) //max vals in toy grid: 202, 135
    /*
     * TODO: consider (start, stride) vs (scale, offset)
     * alternate constructors
     *   min, max, n
     */
    //domainSet.elements foreach println
    
    /*
     * TODO: define SF.resample with Resampling
     * how can we get inter/extrap for general use?
     * 
     * Resampling may or may not include interp
     * 
     * Resample as Operation
     * model will not change
     * need to apply to nested function
     *   how should API indicate that?
     */
    // Resample nested Functions
    // map over outer samples
 
    val resampleGrid = (sample: Sample) => sample match {
      case Sample(domain, RangeData(sf: MemoizedFunction)) =>
        val grid = BinResampling().resample(sf, domainSet)
        Sample(domain, RangeData(grid))
    }

    val data = ds.data.map(resampleGrid) // < 0.5 s
    //TODO: fill missing cells via interp
    
    val model = ds.model
    
    val md = ds.metadata //TODO: enhance, prov
    val ds2 = Dataset(md, model, data)
    //TextWriter().write(ds2)
       
    val pivot = RGBImagePivot("band", 1.0, 1.0, 1.0)
    //val pivot = RGBImagePivot("band", 1.0, 4.0, 3.0)
    val ds3 = pivot(ds2)

    //TextWriter().write(ds3)
    ImageWriter("/data/modis/modisRGB3.png").write(ds3)

    /*
     * TODO: getting OOM trying to get here with a small grid
     * in spark, fast without spark but no support for union, yet
     * OOM even without union, only 2 bands
     * seems to happen in pivot
     * org.apache.spark.rpc.RpcTimeoutException: Futures timed out after [10 seconds]. This timeout is controlled by spark.executor.heartbeatInterval
     *   could the time out cause it to retry using up even more memory to GC?
     * odd that we don't see this in the substitution phase where it takes 10s to read the geoloc data
     * the pivot is happening with the smaller grid
     * reduce 60x50 grid to 6x5: still fails
     * try bumping up timeout from 10 to 30s: error trying to re-register with master
     *   20s: timeout
     * try without substitution (index space), 2 bands, no union, resample 203x135 < 10s!
     *   union, 7 bands: ~50s
     * does non-cartesian lon-lat complicate things? doesn't look like it
     *   
     * 2 bands, no union, no spark, toy grid
     *   toy grid is fast
     *   reading data ~14s
     *   substitution: 245s! toy grid was SeqSF so slow to eval; with ArrayFunction2D < 18s
     *   resample 200x130: < 18s
     *  *with spark, print after resample: 154 s! note, 2 bands sequentially
     *     due to data moving? or memory pressure?
     *     with data as Arrays instead of Vector: 93s
     *       but HashPartitioner cannot partition array keys, use custom partitioner?
     *   with pivot and image gen: OOM after 132 s
     *  *fails in pivot before trying to write
     *  *so not related to skewed grid
     *   the full image gen for 300x250 works without spark < 15 s
     * 
     * TODO: Is this a spark laziness issue?
     * or lack of caching and redoing a lot of work?
     *   note that we pivot on the outer variable: band
     *     band -> (x, y) -> f
     *   each band is in a separate sample in the RDD
     *   might that be the source of the problem?
     *   RGBImagePivot evals at a given band, should be a cheap lookup
     *     then joins by getting the samples of all, sounds costly
     * 
     * look at size of Samples as compressed with Kryo
     *   are we using that much less space with index vs lon-lat?
     *   what kind of SF are we using for the grids? Seq, Indexed?
     *   See TestKryo
     * 
     */
  }
  
  @Test
  def transpose() = {
    /*
     * w -> (x, y) -> f  =>  (x, y) -> w -> f
     * keep spectra together instead of pivoting across outer samples
     * cheapest way in spark?
     *   scatter all then groupBy?
     *   
     * TODO: consider cost of regridding
     * just a key lookup for equality
     * bin resampling with partitioner should be OK
     */
    val ds0 = ModisReader().getDataset  // (band, ix, iy) -> radiance
    val ds1 = GroupBy("ix", "iy")(ds0)  // (ix, iy) -> band -> radiance
    val ds2 = Pivot(List(1.0, 4.0, 3.0), List("r","g","b"))(ds1) // (ix, iy) -> (r, g, b)
    //val ds2 = Pivot(List(1.0, 1.0, 1.0), List("r","g","b"))(ds1) // (ix, iy) -> (r, g, b)

    // Define regular grid to resample onto
    val (nx, ny) = (300, 250)
    val domainSet = LinearSet2D(0.1, -110, nx, 0.1, 10, ny)

    val csx: DomainData => DomainData = geoLocation.data match {
      case f: MemoizedFunction => (dd: DomainData) =>
        f(dd).get.map(_.asInstanceOf[OrderedData]) //TODO: clean up
    }
    val data = BinResampling().resample(ds2.data, domainSet, csx) //26s vs 170! over substitution
    /*
     * TODO: instead of substitution
     * bin resample simply needs stream of samples
     * substitute on the fly instead of making new RDD
     *   data:      (i, j) -> a
     *   csx:       (i', j') -> (lon', lat')  as ArrayFunction2D, fast to eval
     *   domainSet: (lon, lat) => cell index
     *   new grid:  (lon, lat) -> a'
     *   index = domainSet.indexOf(csx(data_sample.domain))
     * optional csx arg to resample?
     * without spark the transposed approach improved from 15 to 12s
     */
    val model = ds2.model //not really
    val md = ds2.metadata //TODO: enhance, prov
    val ds4 = Dataset(md, model, data)
    //TextWriter().write(ds4)   
    ImageWriter("/data/modis/modisRGB4.png").write(ds4)
  }
}
