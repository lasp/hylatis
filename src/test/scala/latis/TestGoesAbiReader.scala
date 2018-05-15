package latis

import org.junit._
import org.junit.Assert._
import scala.io.Source
import latis.input.{GoesAbiNetcdfAdapter, GoesAbiNetcdfReader}
import latis.output._
import latis.metadata._
import latis.data._
import latis.ops._
import java.net.URL
import java.net.URI
import java.io.File

class TestGoesAbiReader {
  val reader = new GoesAbiNetcdfReader
  val adapter = reader.adapter
  
  @Test
  def convertColorToInt: Unit = {
    val red = adapter.Color(255, 0, 0)
    val green = adapter.Color(0, 255, 0)
    val blue = adapter.Color(0, 0, 255)
    val yellow = adapter.Color(255, 255, 0)
    val magenta = adapter.Color(255, 0, 255)
    val cyan = adapter.Color(0, 255, 255)
    val black = adapter.Color(0, 0, 0)
    val white = adapter.Color(255, 255, 255)
    
    assertEquals(16711680, adapter.colorToInt(red))
    assertEquals(65280, adapter.colorToInt(green))
    assertEquals(255, adapter.colorToInt(blue))
    assertEquals(16776960, adapter.colorToInt(yellow))
    assertEquals(16711935, adapter.colorToInt(magenta))
    assertEquals(65535, adapter.colorToInt(cyan))
    assertEquals(0, adapter.colorToInt(black))
    assertEquals(16777215, adapter.colorToInt(white))
  }
  
  @Test
  def interpolateColor: Unit = {
    assertEquals(adapter.Color(128, 128, 0), adapter.interpolateColor(adapter.radianceColors, 350) )
    assertEquals(adapter.Color(0, 255, 0), adapter.interpolateColor(adapter.radianceColors, 400) )
    assertEquals(adapter.Color(0, 128, 128), adapter.interpolateColor(adapter.radianceColors, 450) )
  }
  
  @Test
  def goesDataset: Unit = {
    val data = reader.data
    val metadata = reader.metadata
    val dataset = Dataset(metadata, data)
    assertTrue(dataset.samples.length > 0)
  }
  
}