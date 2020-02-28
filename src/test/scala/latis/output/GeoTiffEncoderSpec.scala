package latis.output

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import geotrellis.vector._

import latis.data.Data.ByteValue
import latis.data._
import latis.dataset.MemoizedDataset
import latis.metadata.Metadata
import latis.model._

class GeoTiffEncoderSpec extends FlatSpec {
  import GeoTiffEncoderSpec.image2D_2x2
  val enc = new GeoTiffEncoder

  "A GeoTiff encoder" should "encode an RGB Dataset to GeoTiff" in {
    val redArr: Array[Byte]   = Array(-1, 0, 0, 0, -1, -127)
    val greenArr: Array[Byte] = Array(0, -1, 0, 0, -1, -127)
    val blueArr: Array[Byte]  = Array(0, 0, -1, 0, -1, -127)

    val tif = enc.encode(image2D_2x2).compile.toList.unsafeRunSync().head

    tif.extent should be(Extent(-104.5, 39.5, -101.5, 41.5))
    tif.tile.band(0).toArray() should be(redArr)
    tif.tile.band(1).toArray() should be(greenArr)
    tif.tile.band(2).toArray() should be(blueArr)
  }
}

object GeoTiffEncoderSpec {
  private def makeRange(r: Byte, g: Byte, b: Byte): RangeData =
    RangeData(ByteValue(r), ByteValue(g), ByteValue(b))

  val image2D_2x2: MemoizedDataset = {
    // Image should look like:
    //   R G B
    //   K W Grey
    // geolocation is North East corner of Colorado
    val samples = Seq(
      Sample(DomainData(40, -104), makeRange(0, 0, 0)),          // black
      Sample(DomainData(41, -104), makeRange(-1, 0, 0)),         // red
      Sample(DomainData(40, -103), makeRange(-1, -1, -1)),       // white
      Sample(DomainData(41, -103), makeRange(0, -1, 0)),         // green
      Sample(DomainData(40, -102), makeRange(-127, -127, -127)), // grey
      Sample(DomainData(41, -102), makeRange(0, 0, -1))          // blue
    )

    val md = Metadata("image2D_2x2")
    val model = Function(
      Tuple(
        Scalar(Metadata("lat") + ("type" -> "double")),
        Scalar(Metadata("lon") + ("type" -> "double"))
      ),
      Tuple(
        Scalar(Metadata("r") + ("type" -> "byte")),
        Scalar(Metadata("g") + ("type" -> "byte")),
        Scalar(Metadata("b") + ("type" -> "byte"))
      )
    )
    val data = SampledFunction(samples)

    new MemoizedDataset(md, model, data)
  }
}
