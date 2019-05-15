package latis

import io.findify.s3mock._
import org.junit._
import org.junit.Assert._
import scala.io.Source
import latis.input._
import latis.output._
import latis.util.AWSUtils
import latis.util.SparkUtils
import latis.metadata._
import latis.model._
import latis.data._
import latis.ops._
import java.net.URL
import java.net.URI
import java.io.File
import java.awt.Color
import latis.ops.Operation
import latis.ops.Uncurry
import cats.effect.IO
import latis.util.GOESUtils.GOESGeoCalculator
import fs2._
import latis.util.StreamUtils._

class TestImageWriter {

  val image2D_2x2 = {
    // row-col ordering
    //   R G
    //   B W
    val samples = Seq(
      Sample(DomainData(0,0), RangeData(1,0,0)),
      Sample(DomainData(0,1), RangeData(0,1,0)),
      Sample(DomainData(1,0), RangeData(0,0,1)),
      Sample(DomainData(1,0), RangeData(1,1,1)),
    )
    
    val md = Metadata("image2D_2x2")
    val model = Function(
      Tuple(Scalar("row"), Scalar("col")),
      Tuple(Scalar("r"), Scalar("g"), Scalar("b"))
    )
    val data = SampledFunction.fromSeq(samples)
    
    Dataset(md, model, data)
  }
  
  //@Test
  def image = {
    //TextWriter(System.out).write(image2D_2x2)
    ImageWriter("rgb.png").write(image2D_2x2)
  }
  
}