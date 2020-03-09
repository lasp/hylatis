package latis

import java.io.FileOutputStream
import java.net.URI

import io.findify.s3mock._
import org.junit._
import org.scalatest.junit.JUnitSuite

import latis.data._
import latis.dataset._
import latis.dsl.geoGrid
import latis.input._
import latis.metadata._
import latis.model._
import latis.model.Tuple
import latis.ops._
import latis.output._
import latis.util.HysicsUtils._

class TestHysics extends JUnitSuite {

  /**
   * Loads the Hysics data, shapes it into the nominal spectral dataset
   * and caches it.
   *   (x, y, wavelength) -> radiance
   */
  lazy val hysicsDataset: Dataset = {
    import latis.dsl._
    //TODO: update to use s3
    val uri = new URI("file:///data/s3/hylatis-hysics-001/des_veg_cloud")
    val wluri = new URI("file:///data/s3/hylatis-hysics-001/des_veg_cloud/wavelength.txt")
    val wlds = HysicsWavelengthReader.read(wluri)

    // Set of y indices spanning the slit dimension
    val iys: Seq[String] = Range(0, 480, 48).map(_.toString)

    HysicsGranuleListReader
      .read(uri) //ix -> uri
      .stride(420) //4200 total slit images
      .toSpark()
      .withReader(HysicsImageReader) // ix -> (iy, iw) -> radiance
      .uncurry() // (ix, iy, iw) -> radiance
      .contains("iy", iys: _*) //TODO: stride
      //.select("iy < 2") //note: not supported in nested function yet
      //.select("iw < 3")
      .contains("iw", "540", "572", "594") // 630.87, 531.86, 463.79
      //RDD partitioner preserved after filter //TODO: range selections generally impact our partitioning
      .substitute(wlds)
      .substitute(xyCSX)
      // This is the canonical form of the cube, cache here
      .cache2() //TODO: resolve conflict with Dataset.cache
  }

  //@Test
  def read_granules(): Unit = {
    val uri = new URI("file:///data/s3/hylatis-hysics-001/des_veg_cloud")
    val ds = HysicsGranuleListReader
      .read(uri)
      .withOperation(Stride(Seq(1000)))
    TextWriter().write(ds)
  }

  //@Test
  def read_image(): Unit = {
    val uri = new URI("file:///data/s3/hylatis-hysics-001/des_veg_cloud/img4200.txt")
    val ds = HysicsImageReader.read(uri)
      .withOperation(Selection("iy < 3"))
      .withOperation(Selection("iw < 3"))
    TextWriter().write(ds)
  }

  //@Test
  def read_wavelengths(): Unit = {
    val uri = new URI("file:///data/s3/hylatis-hysics-001/des_veg_cloud/wavelength.txt")
    val ds = HysicsWavelengthReader.read(uri)
    TextWriter().write(ds)
  }

  def geoSet: DomainSet = {
    val model = Tuple(
      Scalar(Metadata("lon") + ("type" -> "double")),
      Scalar(Metadata("lat") + ("type" -> "double"))
    )
    val xset = BinSet1D(-108.187, 0.011, 3)
    val yset = BinSet1D(34.70, 0.015, 3)
    new BinSet2D(xset, yset, model)
    //TODO: allow setting model of BinSet2D
  }

  //@Test
  def geo_domain_set(): Unit = {
    geoSet.elements.foreach(println)
  }

  @Test
  def geo_grid() = {
    //val dset = geoGrid((-108.27, 34.68), (-108.195, 34.76), 16)
    //println(dset.min) // -108.27, 34.68
    //println(dset.max) // -108.21375, 34.74

    val dset = geoGrid((0, 2), (10, 4), 4)
    dset.elements.foreach(println)
  }

  @Test
  def geo_rgb_image(): Unit = {
    import latis.dsl._
    val ds = hysicsDataset
    //val t0 = System.nanoTime
    hysicsDataset
      .curry(2) // (x, y) -> (wavelength) -> radiance
      .substitute(geoCSX) // (lon, lat) -> (wavelength) -> radiance
      //.compose(rgbExtractor(2301.7, 2298.6, 2295.5)) //first 3
      .compose(rgbExtractor(630.87, 531.86, 463.79)) //iw = 540, 572, 594
      .groupByBin(geoGrid((-108.27, 34.68), (-108.195, 34.76), 100), HeadAggregation()) //based on HYLATIS-35
      .writeImage("/data/hysics/hysicsRGB.png") //TODO: image flipped in y-dim, grid order is fine
      //.writeText() //new FileOutputStream("/data/tmp.txt"))
    //println((System.nanoTime - t0) / 1000000000)
    //240x210 to 100000: spark: 4s, w/o: 3s but loading was much longer: 3m vs 46s
  }

  @Test
  def geo_eval(): Unit = {
    import latis.dsl._
    hysicsDataset
      .curry(2) // (x, y) -> (wavelength) -> radiance
      .substitute(geoCSX) // (lon, lat) -> (wavelength) -> radiance
      .groupByBin(geoGrid((-108.27, 34.68), (-108.195, 34.76), 4), HeadAggregation())
      .eval(TupleData(-108.27, 34.68)) //Note, range values may depend on thinning
      .writeText()
  }

  //======= legacy =============================================================
  //@Test
  //def make_image = {
  //  // wavelength -> (iy, ix) -> radiance
  //  val defaultURI = "s3://hylatis-hysics-001/des_veg_cloud"
  //  val uri = LatisConfig.getOrElse("hylatis.hysics.base-uri", defaultURI)
  //  val hysics = HysicsReader.read(URI.create(uri))
  //
  //  val image = hysics.withOperation(RGBImagePivot(630.87, 531.86, 463.79))
  //  ImageWriter("/data/hysics/hysicsRGB.png").write(image)
  //}

  ////@Test
  //def bulk_load = {
  //  // wavelength -> (iy, ix) -> radiance
  //  val hysics = HysicsReader().getDataset
  //
  //  val ops: Seq[UnaryOperation] = Seq(
  //    //TODO: selection in nested function not working:  Selection("ix >= 477"),
  //    RGBImagePivot(630.87, 531.86, 463.79),
  //    //XYTransform()
  //  )
  //
  //  //val image = DatasetSource.fromName("hysics").getDataset(ops)
  //  val image = hysics.withOperations(ops)
  //  //TextWriter(System.out).write(image)
  //  ImageWriter("hysicsRGB.png").write(image)
  //}
//  //@Test
//  def inspect_wavelength_file(): Unit = {
//    val lines = Source.fromFile("/data_systems/data/test/hylatis/wavelength.txt").getLines()
//    val ss = lines.next.split(",")
//
//    ss.sliding(2).foreach { w =>
//      val diff = w(0).toFloat - w(1).toFloat
//      println(diff)
//    }
//  }
//
//  //@Test
//  def inspect_image_file(): Unit = {
//    val lines = Source.fromFile("/data_systems/data/test/hylatis/img2001.txt").getLines()
//    val ss = lines.next.split(",")
//    println(ss.length)
//  }
//
////  //@Test
////  def read_image_file_into_spark(): Unit = {
////    val reader = AsciiMatrixReader("file:/data_systems/data/test/hylatis/img2000.txt")
////    val ds = reader.getDataset
////    //Writer3().write(ds)
////    SparkDataFrameWriter.write(ds)
////
////    val spark = SparkUtils.getSparkSession
////    val df = spark.sql(s"SELECT * FROM matrix WHERE row < 3 and col < 3")
////    df.show
////  }
//
//  //@Test
//  def read_hysics(): Unit = {
//    val reader = HysicsLocalReader()
//    val ds = reader.getDataset()
//    SparkDataFrameWriter.write(ds)
//
//    val spark = SparkUtils.getSparkSession
//    val df = spark.sql(s"SELECT * FROM hysics where x < 10 and wavelength = 2298.6")
//    df.show
//  }
//
//  //https://www.rp-photonics.com/rgb_sources.html
//  // 630 nm for red, 532 nm for green, and 465 nm for blue light.
//  //hysics: 630.87, 531.86, 463.79
//  //@Test
//  def rgb = {
//    val reader = HysicsLocalReader()
//    val ds = reader.getDataset()
//    SparkDataFrameWriter.write(ds)
//    val spark = SparkUtils.getSparkSession
//    //val dfRGB = spark.sql(s"SELECT * FROM hysics where x < 2 and wavelength in (630.87, 531.86, 463.79)")
//    //val dfR = spark.sql(s"SELECT * FROM hysics where x < 2 and wavelength = 630.87")
//    //val dfG = spark.sql(s"SELECT * FROM hysics where x < 2 and wavelength = 531.86")
//    //val dfB = spark.sql(s"SELECT * FROM hysics where x < 2 and wavelength = 463.79")
//
//    //val df = dfR.join(dfG, Seq("y", "x"))
//    //val df = dfR.union(dfG)
//
//    //dfRGB.show
//    /*
//+---+---+----------+--------+
//|  y|  x|wavelength|   value|
//+---+---+----------+--------+
//|0.0|0.0|    630.87|0.061348|
//|0.0|0.0|    531.86|0.060825|
//|0.0|0.0|    463.79|0.062387|
//|0.0|1.0|    630.87|0.067032|
//|0.0|1.0|    531.86| 0.07077|
//|0.0|1.0|    463.79|0.075786|
//|1.0|0.0|    630.87|0.060736|
//|1.0|0.0|    531.86|0.060336|
//|1.0|0.0|    463.79|0.060563|
//|1.0|1.0|    630.87|0.066708|
//|1.0|1.0|    531.86|0.071449|
//|1.0|1.0|    463.79|0.076174|
//+---+---+----------+--------+
//Note, order preserved
//     */
//
//    //val df = dfRGB.groupBy("y","x").pivot("wavelength").sum("value") //agg needed though only one sample
//    //more performant (https://databricks.com/blog/2016/02/09/reshaping-data-with-pivot-in-apache-spark.html):
//    //and preserves column order
//    //sample order not preserved so must sort
//    //TODO: how much of this could be assumed based on (y,x) domain?
//    //val df = dfRGB
//    val df = spark.table("hysics")
//                  .filter("x < 210")
//                  .filter("x >= 200")
//                  .filter("wavelength in (630.87, 531.86, 463.79)")
//                  .groupBy("y","x")
//                  .pivot("wavelength", Seq(630.87, 531.86, 463.79)) //improve performance by providing values
//                  .sum("value") //aggregation required but not needed
//                  .sort("y", "x")
//                  .withColumnRenamed("630.87", "R")
//                  .withColumnRenamed("531.86", "G")
//                  .withColumnRenamed("463.79", "B")
//                  //.show
//
//    df.createOrReplaceTempView("rgb")
//    val model = FunctionType("image")(
//      TupleType("")(ScalarType("y"), ScalarType("x")),
//      TupleType("")(ScalarType("R"), ScalarType("G"), ScalarType("B"))
//    )
//    val metadata = Metadata("id" -> "rgb")(model)
//
//    //val sdfa = new SparkDataFrameAdapter(Map("location" -> "rgb"))
////    val sdfa = SparkDataFrameAdapter(model, table="rgb")
////    sdfa.init(metadata, model)
////    val imageds = sdfa.makeDataset
//    //Writer().write(imageds)
//    //ImageWriter("testRGB.jpeg").write(imageds)
//    //TODO: jpeg is all black
////    ImageWriter("testRGB.png").write(imageds)
//
//   /*
//    * TODO: avoid shuffling
//    * since there will be no duplicates
//    *   we do not need to aggregate with "sum"
//    *   we don't need to groupBy(y,x)?
//    * we may end up with null wavelength cells
//    * fill when collecting?
//    * maybe groupBy is OK if late enough since we need to collect anyway
//    *   the filtering will have taken place
//    * implications on when/how to sort?
//    *
//    * with FDM, shouldn't have to groupBy
//    *   and samples are unique so no need for agg
//    *   sorting should also be automatic
//    * required for spark API
//    *   pivot expects RelationalGroupedDataset, and returns one
//    *   can we avoid this?
//    *   override sql.Dataset?
//    *   or RDD?
//    */
//
//    /*
//+---+---+--------+--------+--------+
//|  y|  x|       R|       G|       B|
//+---+---+--------+--------+--------+
//|0.0|0.0|0.061348|0.060825|0.062387|
//|0.0|1.0|0.067032| 0.07077|0.075786|
//|1.0|0.0|0.060736|0.060336|0.060563|
//|1.0|1.0|0.066708|0.071449|0.076174|
//+---+---+--------+--------+--------+
//     */
//  }
//
//  //@Test
//  def image_via_adapter = {
//    //load "hysics" data frame in spark
//    val reader = HysicsLocalReader()
//    val ds = reader.getDataset()
//    SparkDataFrameWriter.write(ds)
//
//    //get image dataset via adapter
//    //val ops = Seq(HysicsImageOp(0, 10, 0, 10))
//    val ops = Seq(
//      Select("x >= 0"),
//      Select("x < 3"),
//      Select("y >= 0"),
//      Select("y < 3"),
//      PivotWithValues("wavelength", Seq("630.87", "531.86", "463.79"), Seq("R", "G", "B"))
//    )
//    val imageds = DatasetSource.fromName("hysics").getDataset(ops)
///*
// * TODO: adapter needs new model after pivot...
// * ideally spark would preserve FDM
// * trying to munge just the model locally in the adapter is problematic
// *   we don't have a good way for the adapter to change the model with current life cycle
// * but want to be lazy about applying ops to data
// * need to eagerly munge model so it can be used to construct dataset
// * op.applyToModel ?
// *   depends on type of Op
// *   filter and selection don't affect model
// * See notes in SparkDataFrameAdapter.scala
// *
// */
//    Writer().write(imageds)
//    //write to png image
//    //ImageWriter("testRGB.png").write(imageds)
//    //TODO: look at warnings: Stage 0 contains a task of very large size (181806 KB). The maximum recommended task size is 100 KB.
//  }
//
//  //@Test
//  def operation_regex = {
//    val NUM = """\d+"""
//    val pattern = s"getImage\\(($NUM),($NUM),($NUM),($NUM)\\)"
//    val s = "getImage(0,10,0,10)"
//    val ms = pattern.r.findFirstMatchIn(s) match {
//      case Some(m) => m.subgroups
//    }
//    assertEquals(4, ms.length)
//  }
//
//  //@Test
//  def reflection = {
//    val ds = DatasetSource.fromName("ascii2").getDataset()
//    Writer().write(ds)
//  }
//
//  //@Test
//  def file_list = {
//    val ds = DatasetSource.fromName("hysics_des_veg_cloud_image_files").getDataset()
//    val baseURL = ds.getProperty("baseURL", "")
//    ds.samples foreach {
//      case Sample(_, d) => d match { //TODO: can't do nested match on Text here
//        case Text(file) =>
//          val key = file
//          val obj = new File(new URI(s"$baseURL/$file"))
//          println(s"$key  $obj")
////          s3.putObject("hylatis-hysics-001", key, obj)
//      }
//    }
//    //Writer().write(ds)
//  }
//
//  //@Test
//  def read_wavelengths = {
//    val s3 = AWSUtils.s3Client.get
//    val is = s3.getObject("hylatis-hysics-001", "des_veg_cloud/wavelength.txt").getObjectContent
//    Source.fromInputStream(is).getLines foreach println
//  }
//
//  //@Test //fails with 403, need to set bucket policies
//  def read_wavelengths_via_http = {
//    val uri = new URI("http://hylatis-hysics-001.s3.amazonaws.com/des_veg_cloud/wavelength.txt")
//    val is = uri.toURL.openStream()
//    Source.fromInputStream(is).getLines foreach println
//  }
//
//  //@Test
//  def list_bucket = {
//    import scala.collection.JavaConversions._
//    val s3 = AWSUtils.s3Client.get
//    val os = s3.listObjects("hylatis-hysics-001").getObjectSummaries
//    println(os.length)
//    os.foreach(o => println(o.getKey))
//  }
//
//  //@Test
//  def rdd_of_samples = {
//    val reader = HysicsLocalReader()
//    val ds = reader.getDataset()
//    new SparkWriter().write(ds)
//    //Writer().write(ds)
//
//    //val rdds =  SparkUtils.getSparkSession.sparkContext.getPersistentRDDs
//    //Map of some Int to the RDD;  filter on name;  or use getRDDStorageInfo to get Int first?
//    //println(rdds)
//    val ops = Seq(
//      Select("x < -2082")
//     ,Select("x > -2100")
//     ,Select("y > 94")
//     ,Select("y < 95")
//     ,Select("wavelength = 630.87")
//    )
//    val ds2 = HysicsSparkReader().getDataset(ops)
//    Writer().write(ds2)
//  }
//
//  //@Test
//  def s3mock = {
//    val s3 = AWSUtils.s3Client.get
//    val is = s3.getObject("hylatis-hysics-001", "des_veg_cloud/wavelength.txt").getObjectContent
//    Source.fromInputStream(is).getLines foreach println
//  }
//
//  //@Test
//  def xy_rgb_image = {
//    val reader = HysicsLocalReader()
//    val ds = reader.getDataset()
//    //SparkDataFrameWriter.write(ds)
//    new SparkWriter().write(ds)
//
//    val ops = Seq(RGBImagePivot("wavelength", 630.87, 531.86, 463.79))
//    val image = DatasetSource.fromName("hysics").getDataset(ops)
//    ImageWriter("xyRGB.png").write(image)
//  }

}

object TestHysics {
  //TODO: not working? still need to run s3mock outside first
  private val s3mock: S3Mock = S3Mock(port = 8001, dir = "/data/s3")

  //@BeforeClass
  def startS3Mock: Unit = s3mock.start

  //@AfterClass
  def stopS3Mock: Unit = s3mock.stop
}
