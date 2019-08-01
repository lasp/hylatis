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
  
  //@Test
  def geolocation() = {
    val gds = ModisGeolocationReader().getDataset // (ix, iy) -> (lon, lat), NOT Cartesian
    val gdsa = gds.restructure(ArrayFunction2D) //reading geoloc data into 2D array ~ 10s
    
    //TextWriter(System.out).write(gdsa) //.data.unsafeForce.samples.take(100) foreach println
    
    //TODO: NetcdfFunction.force => ArrayFunction
    //val a = gdsa.data
    //val r = a(DomainData(1000, 1000)) //range has extra Vector
    //println(r)
    //TextWriter(System.out).write(ds)
    /*
     * TODO: substitute into index based modis data
     * 2D substitution could get tricky
     * need to name the x,y tuple and preserve it unnested after curry
     */
    
//    //pick off range of first sample: (ix, iy) -> radiance
//    val sub = Substitution()
//    val ds = {
//      ModisReader().getDataset match {
//        case Dataset(md, model, data) =>
//          val model2 = model match {
//            case Function(_, f: Function) => f //sub.applyToModel(f, gds.model)
//          }
//          val data2 = data(DomainData(1.0)) match {
//            case Some(RangeData(sf: SampledFunction)) => sf
//          }
//          Dataset(md, model2, data2)
//      }
//    }
//
//    val dsg = sub(ds, gdsa)
    //TextWriter(System.out).write(dsg)
  }
  
  /*
   * TODO: load into Spark
   * we ultimately want w -> (x, y) -> f
   * we now read the 3D grid the curry before putting into spark
   *   ok but possible memory limits
   * could load a smaller dataset then read the bands in parallel, akin to goes?
   *   define a dataset for each band using netcdf slice on 1st dimension
   */
  
  @Test
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
   //TextWriter(System.out).write(ds)
    
    // Define regular grid to resample onto
    val (nx, ny) = (60,50)
    val domainSet = LinearSet2D(0.5, -110, nx, 0.5, 10, ny)
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
    
    val model = ds.model
    
    val md = ds.metadata //TODO: enhance, prov
    val ds2 = Dataset(md, model, data)
    //TextWriter().write(ds2)
       
    val pivot = RGBImagePivot("band", 2.0, 1.0, 1.0) // ~ 0.1 s
    val ds3 = pivot(ds2)

println(System.nanoTime()) //
    //TextWriter().write(ds3)
    ImageWriter("/data/modis/modisRGB.png").write(ds3)

    /*
     * TODO: getting OOM trying to get here with a small grid
     * in spark, fast without spark but no support for union, yet
     * OOM even without union, only 2 bands
     * seems to happen in pivot
     * org.apache.spark.rpc.RpcTimeoutException: Futures timed out after [10 seconds]. This timeout is controlled by spark.executor.heartbeatInterval
     * odd that we don't see this in the substitution phase where it takes 10s to read the geoloc data
     * the pivot is happening with the smaller grid
     */
  }
}
