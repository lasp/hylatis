import org.junit._
import scala.io.Source
import latis.reader.AsciiMatrixReader
import latis.writer.SparkDataFrameWriter
import latis.util.SparkUtils
import latis.reader.HysicsReader

class TestHysics {
  
  //@Test
  def inspect_wavelength_file: Unit = {
    val lines = Source.fromFile("/data_systems/data/test/hylatis/wavelength.txt").getLines()
    val ss = lines.next.split(",")
    
    ss.sliding(2).map(w => {
      val diff = w(0).toFloat - w(1).toFloat
      println(diff)
    }).toList
  }
  
  //@Test
  def inspect_image_file: Unit = {
    val lines = Source.fromFile("/data_systems/data/test/hylatis/img2001.txt").getLines()
    val ss = lines.next.split(",")
    println(ss.length)
  }
  
  //@Test
  def read_image_file_into_spark = {
    val reader = AsciiMatrixReader("file:/data_systems/data/test/hylatis/img2000.txt")
    val ds = reader.getDataset
    //Writer3().write(ds)
    SparkDataFrameWriter.write(ds)
    
    val spark = SparkUtils.getSparkSession
    val df = spark.sql(s"SELECT * FROM matrix WHERE row < 3 and col < 3")
    df.show
  }
  
  @Test
  def read_hysics = {
    val reader = HysicsReader()
    val ds = reader.getDataset
    SparkDataFrameWriter.write(ds)
    
    val spark = SparkUtils.getSparkSession
    val df = spark.sql(s"SELECT * FROM hysics where x < 3 and wavelength = 2298.6")
    df.show
  }
}