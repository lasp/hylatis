package latis

import org.junit._
import org.junit.Assert._
import latis.input.{GoesAbiNetcdfDisplayAdapter, GoesAbiNetcdfDisplayReader}
import java.awt.Color

class TestGoesAbiReader {
  val reader = 
    new GoesAbiNetcdfDisplayReader("s3://noaa-goes16/ABI-L1b-RadF/2018/071/12/OR_ABI-L1b-RadF-M3C16_G16_s20180711200421_e20180711211199_c20180711211258.nc")
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
  
  @Test
  def goesDataset: Unit = {
    val data = reader.data
    val metadata = reader.metadata
    val dataset = Dataset(metadata, data)
    assertTrue(dataset.samples.length > 0)
  }
  
}