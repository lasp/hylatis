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

class TestModis {
  
  // (iw, ix, iy) -> EV_1KM_RefSB
  val model = Function(
    Tuple(Scalar("iw"), Scalar("ix"), Scalar("iy")),
    Scalar("EV_1KM_RefSB")
  )
  
  @Test
  def read() = {
    val uri = new URI("/data/modis/MYD021KM.A2014230.1940.061.2018054170416.hdf")
    val data = ModisNetcdfAdapter(model)(uri)
    val ds = Dataset(Metadata("modis"), model, data).restructure(RddFunction)
    
    val ds2 = GroupBy("iw")(ds)
    val image = RGBImagePivot("iw", 0,1,2)(ds2)
    //StreamUtils.unsafeStreamToSeq(ds2.data.streamSamples.take(10)) foreach println
    //TextWriter(System.out).write(ds3)
    ImageWriter("modisRGB.png").write(image)
  }
}