package latis.output

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.util.NullOutputStream

class GeoTiffWriterSpec extends FlatSpec {
  import GeoTiffEncoderSpec.image2D_2x2

  "A GeoTiff writer" should "write an RGB Dataset to GeoTiff" in {
    val nullOut = new NullOutputStream()
    try {
      GeoTiffWriter(nullOut).write(image2D_2x2)
    } finally {
      nullOut.close()
    }

  }
}
