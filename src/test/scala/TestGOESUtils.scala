import org.junit._
import org.junit.Assert._

import latis.util.GOESUtils
import latis.util.GOESUtils.GOESGeoCalculator

class TestGOESUtils {
  
  @Test
  def verifyLocation: Unit = {
    assertTrue(GOESUtils.isTargetVisible((-109.0, 41.0)))  // NW corner of Colorado
    assertFalse(GOESUtils.isTargetVisible((16.37, 48.2)))  // Vienna, Austria
    assertTrue(GOESUtils.isTargetVisible((0.13, 51.51)))   // London, England
    
//    val calc = GOESGeoCalculator("GOES_EAST")
//    println("Quito Ecuador:")
//    calc.geoToYX((-78.47, 0.1807))
//    println("NW Corner of Colorado:")
//    calc.geoToYX((-109.0, 41.0))
//    println("Sample Location from Docs:")
//    calc.geoToYX((-84.690932, 33.846162))
  }
}