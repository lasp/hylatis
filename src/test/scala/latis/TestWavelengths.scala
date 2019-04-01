package latis

import org.junit._
import org.junit.Assert._
import latis.model._
import latis.data._

class TestWavelengths {
  
  @Test
  def read() = {
    //val ds = HysicsWavelengthsReader(new URI("s3://hylatis-hysics-001/des_veg_cloud/wavelength.txt")).getDataset
    val ds = Dataset.fromName("hysics_wavelengths") //HysicsWavelengths()
    //Writer.write(ds)
    ds match {
      case Dataset(_,_,sf) => sf(DomainData(631)) match {
        case MemoizedFunction(ss) => ss.head match {
          case Sample(_, RangeData(w: Double)) =>
            assertEquals(349.3, w, 0)
        }
      }
    }
  }
  
  
}