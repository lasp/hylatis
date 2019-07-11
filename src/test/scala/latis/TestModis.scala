package latis

import latis.model._
import org.junit._
import latis.input.ModisNetcdfAdapter
import java.net.URI
import latis.metadata.Metadata
import latis.util.StreamUtils
import latis.ops.GroupBy
import latis.output.TextWriter
import latis.ops.RGBImagePivot
import latis.output.ImageWriter
import latis.data.RddFunction
import latis.input.ModisReader

class TestModis {
  
  @Test
  def reader() = {
    val uri = new URI("/data/modis/MYD021KM.A2014230.1940.061.2018054170416.hdf")
    val ds = ModisReader(uri).getDataset
    //TextWriter(System.out).write(ds)
    val image = RGBImagePivot("band", 1.0, 4.0, 2.0)(ds)
    //TextWriter(System.out).write(image)
    ImageWriter("/data/modis/modisRGB.png").write(image)
  }
}