package latis.ops

import org.junit._
import org.junit.Assert._
import org.scalatest.junit.JUnitSuite

import latis.data._

class TestBoundingBoxEvaluation extends JUnitSuite {
  
  @Test
  def array2d() = {
    val f = IndexedFunction2D(
      Seq(1.0,2.0,3.0),
      Seq(1.0,2.0,3.0,4.0),
      Seq(
        Seq(1.0,2.0,3.0,4.0).map(d => RangeData(Data(d))),
        Seq(5.0,6.0,7.0,8.0).map(d => RangeData(Data(d))),
        Seq(9.0,10.0,11.0,12.0).map(d => RangeData(Data(d)))
      )
    )
    
    val bbox = BoundingBoxEvaluation(1.0,1.0,3.0,3.0,4)
    val f2 = bbox.applyToData(f, null)
    f2.unsafeForce.samples.last match { //.foreach(println(_))
      case Sample(_, RangeData(Number(v))) => assertEquals(6.0, v, 0)
    }
  }
}