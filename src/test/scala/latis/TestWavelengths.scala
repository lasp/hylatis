package latis

import org.junit._
import org.junit.Assert._
import org.scalatest.junit.JUnitSuite

import latis.model._
import latis.data._
import latis.dataset.Dataset

class TestWavelengths extends JUnitSuite {

  @Test
  def read() = {
    //val ds = HysicsWavelengthsReader(new URI("s3://hylatis-hysics-001/des_veg_cloud/wavelength.txt")).getDataset
    val ds = Dataset.fromName("hysics_wavelengths") //HysicsWavelengths()
    //Writer.write(ds)
    ds.unsafeForce().data.sampleSeq.head match {
      case Sample(_, RangeData(Number(w))) =>
        assertEquals(349.3, w, 0)
    }
  }

}
