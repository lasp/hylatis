package latis

import org.junit._
import org.scalatest.junit.JUnitSuite

import latis.output._
import latis.model._
import latis.data._
import latis.dataset._
import latis.metadata.Metadata

class TestImageWriter extends JUnitSuite {

  def image2D_2x2 = {
    // Image should look like:
    //   R G B
    //   K W Grey
    val samples = Seq(
      Sample(DomainData(0, 0), RangeData(0, 0, 0)),       // black
      Sample(DomainData(0, 0.5), RangeData(1, 0, 0)),     // red
      Sample(DomainData(0.5, 0), RangeData(1, 1, 1)),     // white
      Sample(DomainData(0.5, 0.5), RangeData(0, 1, 0)),   // green
      Sample(DomainData(1, 0), RangeData(0.5, 0.5, 0.5)), // grey
      Sample(DomainData(1, 0.5), RangeData(0, 0, 1))      // blue
    )

    val md = Metadata("image2D_2x2")
    val model = Function(
      Tuple(
        Scalar(Metadata("x") + ("type" -> "double")),
        Scalar(Metadata("y") + ("type" -> "double"))
      ),
      Tuple(
        Scalar(Metadata("r") + ("type" -> "double")),
        Scalar(Metadata("g") + ("type" -> "double")),
        Scalar(Metadata("b") + ("type" -> "double"))
      )
    )
    val data = SampledFunction(samples)

    new MemoizedDataset(md, model, data)
  }

//  @Test
  def image =
    //TextWriter(System.out).write(image2D_2x2)
    ImageWriter("rgb.png").write(image2D_2x2)

}
