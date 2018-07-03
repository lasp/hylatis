package latis

import org.junit._
import org.junit.Assert._
import latis.input.{GoesAbiNetcdfDisplayAdapter, GoesAbiNetcdfDisplayReader, GoesGranuleListReader}
import latis.output._
import java.awt.Color
import io.findify.s3mock._

class TestGoesAbiReader {
  val uri = "file:///Users/pepf5062/Downloads/AwsTest/OR_ABI-L1b-RadF-M3C16_G16_s20180711200421_e20180711211199_c20180711211258.nc"
  val reader = new GoesAbiNetcdfDisplayReader(uri)
  val adapter = reader.adapter
  
  @Test
  def convertColorToInt: Unit = {
    val red = new Color(255, 0, 0, 255)
    val green = new Color(0, 255, 0, 255)
    val blue = new Color(0, 0, 255, 255)
    val yellow = new Color(255, 255, 0, 255)
    val magenta = new Color(255, 0, 255, 255)
    val cyan = new Color(0, 255, 255, 255)
    val black = new Color(0, 0, 0, 255)
    val white = new Color(255, 255, 255, 255)
    
    assertEquals(-65536, adapter.colorToInt(red))
    assertEquals(-16711936, adapter.colorToInt(green))
    assertEquals(-16776961, adapter.colorToInt(blue))
    assertEquals(-256, adapter.colorToInt(yellow))
    assertEquals(-65281, adapter.colorToInt(magenta))
    assertEquals(-16711681, adapter.colorToInt(cyan))
    assertEquals(-16777216, adapter.colorToInt(black))
    assertEquals(-1, adapter.colorToInt(white))
  }
  
  @Test
  def interpolateColor: Unit = {
    assertEquals(new Color(128, 128, 0, 255), adapter.interpolateColor(adapter.radianceColors, 350) )
    assertEquals(new Color(0, 255, 0, 255), adapter.interpolateColor(adapter.radianceColors, 400) )
    assertEquals(new Color(0, 128, 128, 255), adapter.interpolateColor(adapter.radianceColors, 450) )
  }
  
  //@Test
  def goesDataset: Unit = {
    val data = reader.data
    val metadata = reader.metadata
    val dataset = Dataset(metadata, data)
    assertTrue(dataset.samples.length > 0)    // explicitly calling samples may cause heap to overflow
  }
  
  @Test
  def bulk_load = {
    val reader = GoesGranuleListReader()
    val ds = reader.getDataset()
    new SparkWriter().write(ds)
    
    /*val ops: Seq[Operation] = Seq(
      Select("iy < 10")
      /*
       * TODO: URI => (ix, iw) -> f  compose?
       *   that is just the adapter.apply
       * or map function over entire samples
       */
      , HysicsImageReaderOperation()
      /*
       * TODO: Select only works if uncurried, so do it first
       * TODO: uncurry doesn't flatten domain type so we have (y,(x,w)) which confuses the writer
       *   should uncurry flatten the type or should writer be more forgiving?
       *   seems like we should preserve type for things like GeoLocation or Vector tuples
       *   review writer in context of new flattened Sample
       *   
       * hysics_image_files.png?iy<10&read()&uncurry()&ix<10&rgbPivot("iw", 100, 200, 300)
       */
      , Uncurry()
      , Select("ix < 10")
      , RGBImagePivot("iw", 100, 200, 300)
    )
    val image = HysicsSparkReader().getDataset(ops)
    //Writer().write(sds)
    //sds.samples foreach println
    //val image = DatasetSource.fromName("hysics").getDataset(ops)
    ImageWriter("indexRGB.png").write(image)*/
  }
  
}


object TestGoes {
  
  private val s3mock: S3Mock = S3Mock(port = 8001, dir = "/data/s3")
  
  @BeforeClass
  def startS3Mock: Unit = s3mock.start
  
  @AfterClass
  def stopS3Mock: Unit = s3mock.stop
}