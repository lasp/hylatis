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
import java.io.FileOutputStream
import latis.input.ModisGeolocationReader
import latis.ops.Substitution
import latis.ops._
import scala.util.Random
import latis.input.ModisGranuleListReader
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import java.io.ObjectOutputStream

class TestModis {
//object TestModis extends App {
  
  //@Test
  def reader() = {
    //Note: we can override the config:
    //System.setProperty("hylatis.modis.uri", "/data/modis/MYD021KM.A2014230.2120.061.2018054180853.hdf")
    val ds = ModisReader().getDataset
    //TextWriter(System.out).write(ds)
    val image = RGBImagePivot(1.0, 1.0, 1.0)(ds)
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
  
  lazy val geoLocation = ModisGeolocationReader().getDataset // (ix, iy) -> (lon, lat)

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
    //val domainSet = LinearSet2D(1, -110, nx, 1, 10, ny)
    val domainSet = BinSet2D(
      BinSet1D(-110, 1, nx),
      BinSet1D(10, 1, ny)
    )
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
       
    val pivot = RGBImagePivot(1.0, 1.0, 1.0)
    //val pivot = RGBImagePivot(1.0, 4.0, 3.0)
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
     *     but GOES does this?
     * 
     * look at size of Samples as compressed with Kryo
     *   are we using that much less space with index vs lon-lat?
     *   what kind of SF are we using for the grids? Seq, Indexed?
     *   See TestKryo
     * 
     */
  }
  


  @Test
  def from_granules() = {
    //val granules = ModisGranuleListReader().getDataset.restructure(RddFunction) // band -> uri
    //val ds0 = ModisBandReaderOperation()(granules) //band -> (ix, iy) -> radiance
    //val ds1 = ModisGeoSub()(ds0) //band -> (longitude, latitude) -> radiance
    //println(ds1.data.asInstanceOf[RddFunction].rdd.count) // 18s for 7 bands with stride=10, 22s for 38 bands
    
    //val ds1a = Contains("band", 1.0, 3.0, 5.0)(ds1)
    //val ds1a = Selection("band < 6")(ds1)
    //TextWriter().write(ds1a)
    
    val ds1 = ModisReader().getDataset
    
    val domainSet = BinSet2D.fromExtents((-110, 10), (-80, 35), 75000)
    //domainSet.elements foreach println
    val ds2 = Resample(domainSet)(ds1) //TODO: avoid regridding all bands

    val ds3 = RGBImagePivot(1.0, 5.0, 3.0)(ds2) // (longitude, latitude) -> (r, g, b)

    //val ds3 = GroupByBin(domainSet, NearestNeighborAggregation())(ds2)
    //TextWriter().write(ds3)
    ImageWriter("/data/modis/rgbImage2.png").write(ds3)
  }
  
  
  //@Test
  def transposed() = {
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
    val ds0 = ModisReader().getDataset                // (band, ix, iy) -> radiance
    val ds0a = Substitution()(ds0, geoLocation)       // (band, longitude, latitude) -> radiance
 .restructure(RddFunction)
    val ds1 = GroupBy("longitude", "latitude")(ds0a)  // (longitude, latitude) -> band -> radiance
    val ds2 = Pivot(List(1.0, 4.0, 5.0), List("r","g","b"))(ds1) // (longitude, latitude) -> (r, g, b)
    
    // Define regular grid to resample onto
    val s: Int = 1
    val (nx, ny) = (30/s, 25/s)
    //val (nx, ny) = (300/s, 250/s)
    //val domainSet = LinearSet2D(0.1*s, -110, nx, 0.1*s, 10, ny)  // (longitude, latitude)
    val domainSet = BinSet2D(
      BinSet1D(-100, 0.2*s, nx),
      BinSet1D(20, 0.2*s, ny)
    )

    val ds3 = GroupByBin(domainSet, NearestNeighborAggregation())(ds2)
    //TextWriter().write(ds3)
    ImageWriter("/data/modis/modisRGB.png").write(ds3)

    /*
     * 2019-08-13: still getting OOM in spark even with stride of 100 and 3x2 domainSet
     * See OOMerror file
     * No warnings until crash
     * MemoryStore: MemoryStore started with capacity 2004.6 MB
     *   most of it remained free
     * 16:39:46 INFO SparkContext: Starting job: sortBy at RddFunction.scala:160
     *   this is part of the union of the band chunks
     *   8 partitions
     *   3 ShuffleMapStages?, last finished at 16:39:48
     * 16:39:47 INFO DAGScheduler: Job 0 finished: sortBy at RddFunction.scala:160, took 1.457613 s
     * 16:39:47 INFO SparkContext: Starting job: sortBy at RddFunction.scala:113
     *   this is part of the group by, for (ix, iy)?
     * 16:40:33 INFO SparkContext: Starting job: sortBy at RddFunction.scala:81
     *   this is part of the GroupByBin
     * 16:41:59 WARN NettyRpcEnv: Ignored message: HeartbeatResponse(false)
     * 16:41:59 ERROR Utils: uncaught error in thread Spark Context Cleaner, stopping SparkContext
     *   java.lang.OutOfMemoryError: GC overhead limit exceeded
	   *   at org.apache.spark.ContextCleaner$$Lambda$574/95304344.get$Lambda(Unknown Source)
	   * 16:41:59 WARN Executor: Issue communicating with driver in heartbeater
     *   org.apache.spark.rpc.RpcTimeoutException: Cannot receive any reply from cu-biot-3-10.203.138.230.int.colorado.edu:57114 in 10 seconds. 
     *     This timeout is controlled by spark.executor.heartbeatInterval
	   * 16:42:12 ERROR Executor: Exception in task 0.0 in stage 10.0 (TID 40)
     *   java.lang.OutOfMemoryError: GC overhead limit exceeded
	   *   at java.lang.reflect.Array.newInstance(Array.java:75)
	   *   at java.io.ObjectInputStream.readArray(ObjectInputStream.java:1671)
	   *   at java.io.ObjectInputStream.readObject0(ObjectInputStream.java:1345)
	   *   at java.io.ObjectInputStream.defaultReadFields(ObjectInputStream.java:2000)
	   *   and a full stack like these
	   *   why is this so much later than orig OOM?
	   *   
	   * calling persist after the first groupby didn't help
	   * 
	   * How much should we be shuffling for this limited case?
	   * binning 20x13 samples into 6 bins
	   * each sample should have a spectrum of 7 bands
	   * 
	   * ??????????????????????????????????????????????????????????
	   * 
     */
    
    
    /*
     * TODO: Use GroupByBin
     * compose with csx
     *   avoid making new samples that have to be shuffled
     * gbf: Sample => Option[DomainData]
     * agg: (DomainData, Iterable[Sample]) => Sample
     * csx: DomainData => DomainData
     * would need to get DD of gbf Sample and apply csx to it
     * is there only one way to use "withCSX"
     *   might want to apply csx to output DD
     * can we simply prepend a MappingOperation? e.g. Substitution
     *   withMapping(f: S=>S)
     *   "Pre"? but would only do "post" if this were a MapOp so compose the other way around
     *   would need entire Op to manage model...
     *   "composeWith(mop: MappingOperation)"?
     *   how to apply:
     *     only if op uses stream or iterable of Samples?
     *     but can RDD or other SFs override?
     *       e.g. jdbc selection won't apply filter to samples
     *         but it is a source and wouldn't be preceded by a mapping
     * 
     * SF with CSX:
     *   apply when providing samples
     *   eval: apply to args, could offer either CS
     *   but can't  apply to model, need Op like Substitution
     *   
     * Op.withMapping could call SF.map
     * 
     * Primary use case: avoid extra shuffling in spark
     * the extra map wouldn't help
     * needs to be composed with the map function
     *   or in this case the groupByFunction: Sample => Option[DomainData]
     * Substitution is a BinaryOp that can be partially applied with the csx to make a MapOp
     *   how can we know that it can be a MapOp? do we need BinMapOp?
     *   compiler could inspect and cast via pattern match
     *  *actually csx needs to be 2nd dataset
     *   
     * Does it need to be as general as Subst? more like composition
     *   would need to invert csx to then compose
     *   csx would generally have to be algorithm
     *   inversion not cheap
     *   but need to start with domain in index space to iterate samples
     *   substitution sounds like the right concept
     *   maybe we could specialize for a csx subst, replace domain only
     *     not helpful in general?
     * 
     * Just pass in optional CSX to GBB for now?
     */
    
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
//    val model = ds2.model //not really
//    val md = ds2.metadata //TODO: enhance, prov
//    val ds4 = Dataset(md, model, data)
    //TextWriter().write(ds4)   
    //ImageWriter("/data/modis/modisRGB4.png").write(ds3)
  }

//  @Test
  def group_by_bin_2D_without_csx() = {
    
    // x, y -> a
    val model = Function(
      Tuple(
        Scalar(Metadata("id" -> "x", "type" -> "double")),
        Scalar(Metadata("id" -> "y", "type" -> "double"))
      ),
      Scalar(Metadata("id" -> "a", "type" -> "double"))
    )
    
    // Define an irregular 2D grid of data to be GBB'd
    val (nx, ny) = (100, 100) // 1000000: 26s
    val rando = new Random()
    val samples = for {
      _ <- 0 until nx
      _ <- 0 until ny
      x = rando.nextDouble * 10
      y = rando.nextDouble * 10
      v = x + y
    } yield Sample(DomainData(x,y), RangeData(v))
    val data = SeqFunction(samples)
    //samples foreach println
    
    val dataset = Dataset( Metadata("test_grouping2D"), model, data)
    
    val domainSet = ??? //LinearSet2D(1.0, 1.0, 10, 1.0, 1.0, 10) // [1..10]

    val ds = GroupByBin(domainSet, NearestNeighborAggregation())(dataset.restructure(RddFunction))

    TextWriter().write(ds)
  }
    
  // Define CSX dataset: (ix, iy) -> (x, y)
  // regular indices but random x,y
  // size similar to 1km res modis data
  val csx = {
    // (ix, iy) -> (x, y)
    val model = Function(
      Tuple(
        Scalar(Metadata("id" -> "ix", "type" -> "int")),
        Scalar(Metadata("id" -> "iy", "type" -> "int"))
      ),
      Tuple(
        Scalar(Metadata("id" -> "x", "type" -> "double")),
        Scalar(Metadata("id" -> "y", "type" -> "double"))
      )
    )
     
    val (nx, ny) = (2030, 1354)
    val rando = new Random()
    val array: Array[Array[RangeData]] = Array.tabulate(nx, ny)((i,j) => RangeData(rando.nextDouble * 10, rando.nextDouble * 10))
    val data = ArrayFunction2D(array)

    Dataset(Metadata("csx"), model, data)
  }
  
  //@Test
  def group_by_bin_2D_with_csx_early() = {
    
    // ix, iy -> a
    val model = Function(
      Tuple(
        Scalar(Metadata("id" -> "ix", "type" -> "int")),
        Scalar(Metadata("id" -> "iy", "type" -> "int"))
      ),
      Scalar(Metadata("id" -> "a", "type" -> "double"))
    )
    
    // MODIS subset analog
    val (nx, ny) = (100, 100)
    val samples = for {
      ix <- 0 until nx
      iy <- 0 until ny
      v: Double = ix + iy
    } yield Sample(DomainData(ix,iy), RangeData(v))
    val data = SeqFunction(samples)
    //samples foreach println
    
    val dataset = {
      val ds = Dataset(Metadata("test_grouping2D"), model, data)
      Substitution()(ds, csx)
      .restructure(RddFunction)
    }
    
    
    val domainSet = ??? //LinearSet2D(1.0, 1.0, 10, 1.0, 1.0, 10) // [1..10]x[1..10]

    val ds = GroupByBin(domainSet, NearestNeighborAggregation())(dataset)

    TextWriter().write(ds)
  }
  
  //@Test
  def group_by_bin_2D_with_csx() = {
    
    // ix, iy -> a
    val model = Function(
      Tuple(
        Scalar(Metadata("id" -> "ix", "type" -> "int")),
        Scalar(Metadata("id" -> "iy", "type" -> "int"))
      ),
      Scalar(Metadata("id" -> "a", "type" -> "double"))
    )
    
    // MODIS subset analog
    val (nx, ny) = (100, 100)
    val samples = for {
      ix <- 0 until nx
      iy <- 0 until ny
      v: Double = ix + iy
    } yield Sample(DomainData(ix,iy), RangeData(v))
    val data = SeqFunction(samples)
    //samples foreach println
    
    val dataset = Dataset(Metadata("test_grouping2D"), model, data).restructure(RddFunction)
    
    val domainSet = ??? //LinearSet2D(1.0, 1.0, 10, 1.0, 1.0, 10) // [1..10]x[1..10]

    //val ds = GroupByBin(domainSet, NearestNeighborAggregation(), csx)(dataset)

    //TextWriter().write(ds)
  }
  
  //@Test
  def group_by_bin_1D_with_spark() = {
    val dataset = {
      val metadata = Metadata("test_grouping")

      // x -> a
      val model = Function(
        Scalar(Metadata("id" -> "x", "type" -> "double")),
        Scalar(Metadata("id" -> "a", "type" -> "double")))

      val domainSet = BinSet1D(0.0, 0.003, 3000)  
      // 3000: 5s
      // 3000000: 98s
      
      val range = for {
        i <- (0 until domainSet.length)
      } yield RangeData(i * 10.0)
      //val data = SetFunction(domainSet, range)
      // use SeqFunction so we don't get special optimization
      val data = SeqFunction(SetFunction(domainSet, range).samples)

      Dataset(metadata, model, data)
    }
    val domainSet = BinSet1D(1.0, 1.0, 10) // [1..10]

    val ds = GroupByBin(domainSet, NearestNeighborAggregation())(dataset.restructure(RddFunction))

    TextWriter().write(ds)
  }
}
