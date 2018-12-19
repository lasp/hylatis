package latis

import org.junit._
import org.junit.Assert._
import latis.input.HysicsWavelengthsReader
import java.net.URI
import latis.output.Writer
import latis.model._
import latis.data._

class TestWavelengths {
  
  @Test
  def read = {
    val ds = HysicsWavelengthsReader(new URI("file:/data/hysics/des_veg_cloud/wavelength.txt")).getDataset(Seq.empty)
    //Writer.write(ds)
    val z = ds match {
      case Dataset(_,_,sf) => sf(DomainData(0,631)) match {
        case MemoizedFunction(ss) => ss.head match {
          case Sample(_, RangeData(w: Double)) =>
            assertEquals(349.3, w, 0)
        }
      }
    }
  }
  
  
}