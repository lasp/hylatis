package latis

import org.junit._
import org.junit.Assert._
import scala.io.Source
import latis.input._
import latis.output._
import latis.util.SparkUtils
import latis.metadata._
import latis.data._
//import latis.ops.HysicsImageOp
import latis.ops._
import java.net.URL
import java.net.URI
import java.io.File
import latis.util.AWSUtils
import io.findify.s3mock._
import latis.ops.Operation
import latis.ops.Uncurry

class TestHysics {
  
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
  
  @Test
  def bulk_load = {
    // (iy, ix, wavelength) -> irradiance
    val hysics = HysicsReader().getDataset(Seq.empty)
    
    val ops: Seq[Operation] = Seq(
      Selection("ix >= 478")
      , Contains("wavelength", 630.87, 531.86, 463.79)
      , GroupBy("ix", "iy")
   //   , Pivot(Vector(630.87, 531.86, 463.79), Vector("r","g","b"))
   //   , XYTransform()
    )
    
    //val image = DatasetSource.fromName("hysics").getDataset(ops)
    val image = ops.foldLeft(hysics)((ds, op) => op(ds))
    Writer.write(image)
    //ImageWriter("indexRGB.png").write(image)
  }

}

object TestHysics {
  
  private val s3mock: S3Mock = S3Mock(port = 8001, dir = "/data/s3")
  
  //@BeforeClass
  def startS3Mock: Unit = s3mock.start
  
  //@AfterClass
  def stopS3Mock: Unit = s3mock.stop
}