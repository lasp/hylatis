package latis.ops

import org.junit._
import org.junit.Assert._
import latis.data._

class TestBoundingBoxResampling {
  
  @Test
  def array2d() = {
    val f = IndexedFunction2D(
      Array(1.0,2.0,3.0),
      Array(1.0,2.0,3.0,4.0),
      Array(
        Array(1.0,2.0,3.0,4.0),
        Array(5.0,6.0,7.0,8.0),
        Array(9.0,10.0,11.0,12.0)
      )
    )
    
    val bbox = BoundingBoxResampling(1.0,1.0,3.0,3.0,4)
    val f2 = bbox.applyToData(f, null)
    f2.samples.last match { //.foreach(println(_))
      case Sample(_, RangeData(Number(v))) => assertEquals(6.0, v, 0)
    }
  }
}