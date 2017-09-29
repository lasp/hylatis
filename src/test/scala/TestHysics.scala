import org.junit._
import scala.io.Source
import latis.reader.AsciiMatrixReader
import latis.writer.SparkDataFrameWriter
import latis.util.SparkUtils
import latis.reader.HysicsReader

class TestHysics {
  
  //@Test
  def inspect_wavelength_file(): Unit = {
    val lines = Source.fromFile("/data_systems/data/test/hylatis/wavelength.txt").getLines()
    val ss = lines.next.split(",")
    
    ss.sliding(2).foreach { w =>
      val diff = w(0).toFloat - w(1).toFloat
      println(diff)
    }
  }
  
  //@Test
  def inspect_image_file(): Unit = {
    val lines = Source.fromFile("/data_systems/data/test/hylatis/img2001.txt").getLines()
    val ss = lines.next.split(",")
    println(ss.length)
  }
  
  //@Test
  def read_image_file_into_spark(): Unit = {
    val reader = AsciiMatrixReader("file:/data_systems/data/test/hylatis/img2000.txt")
    val ds = reader.getDataset
    //Writer3().write(ds)
    SparkDataFrameWriter.write(ds)
    
    val spark = SparkUtils.getSparkSession
    val df = spark.sql(s"SELECT * FROM matrix WHERE row < 3 and col < 3")
    df.show
  }
  
  //@Test
  def read_hysics(): Unit = {
    val reader = HysicsReader()
    val ds = reader.getDataset
    SparkDataFrameWriter.write(ds)
    
    val spark = SparkUtils.getSparkSession
    val df = spark.sql(s"SELECT * FROM hysics where x < 10 and wavelength = 2298.6")
    df.show
  }
  
  //https://www.rp-photonics.com/rgb_sources.html
  // 630 nm for red, 532 nm for green, and 465 nm for blue light.
  //hysics: 630.87, 531.86, 463.79
  //@Test
  def rgb = {
    val reader = HysicsReader()
    val ds = reader.getDataset
    SparkDataFrameWriter.write(ds)
    val spark = SparkUtils.getSparkSession
    //val dfRGB = spark.sql(s"SELECT * FROM hysics where x < 2 and wavelength in (630.87, 531.86, 463.79)")
    //val dfR = spark.sql(s"SELECT * FROM hysics where x < 2 and wavelength = 630.87")
    //val dfG = spark.sql(s"SELECT * FROM hysics where x < 2 and wavelength = 531.86")
    //val dfB = spark.sql(s"SELECT * FROM hysics where x < 2 and wavelength = 463.79")
        
    //val df = dfR.join(dfG, Seq("y", "x"))
    //val df = dfR.union(dfG)
    
    //dfRGB.show
    /*
+---+---+----------+--------+
|  y|  x|wavelength|   value|
+---+---+----------+--------+
|0.0|0.0|    630.87|0.061348|
|0.0|0.0|    531.86|0.060825|
|0.0|0.0|    463.79|0.062387|
|0.0|1.0|    630.87|0.067032|
|0.0|1.0|    531.86| 0.07077|
|0.0|1.0|    463.79|0.075786|
|1.0|0.0|    630.87|0.060736|
|1.0|0.0|    531.86|0.060336|
|1.0|0.0|    463.79|0.060563|
|1.0|1.0|    630.87|0.066708|
|1.0|1.0|    531.86|0.071449|
|1.0|1.0|    463.79|0.076174|
+---+---+----------+--------+
Note, order preserved
     */
    
    //val df = dfRGB.groupBy("y","x").pivot("wavelength").sum("value") //agg needed though only one sample
    //more performant (https://databricks.com/blog/2016/02/09/reshaping-data-with-pivot-in-apache-spark.html):
    //and preserves column order
    //sample order not preserved so must sort
    //TODO: how much of this could be assumed based on (y,x) domain?
    //val df = dfRGB
    val df = spark.table("hysics")
                  .filter("x < 2")
                  .filter("wavelength in (630.87, 531.86, 463.79)")
                  .groupBy("y","x")
                  .pivot("wavelength", Seq(630.87, 531.86, 463.79)) //improve performance by providing values
                  .sum("value") //aggregation required but not needed
                  .sort("y", "x")
                  .show
                  
   /*
    * TODO: avoid shuffling
    * since there will be no duplicates
    *   we do not need to aggregate with "sum"
    *   we don't need to groupBy(y,x)?
    * we may end up with null wavelength cells
    * fill when collecting?
    * maybe groupBy is OK if late enough since we need to collect anyway
    *   the filtering will have taken place
    * implications on when/how to sort?
    */
    
    /*
+---+---+--------+--------+--------+
|  y|  x|  630.87|  531.86|  463.79|
+---+---+--------+--------+--------+
|0.0|0.0|0.061348|0.060825|0.062387|
|0.0|1.0|0.067032| 0.07077|0.075786|
|1.0|0.0|0.060736|0.060336|0.060563|
|1.0|1.0|0.066708|0.071449|0.076174|
+---+---+--------+--------+--------+
     */
  }
}