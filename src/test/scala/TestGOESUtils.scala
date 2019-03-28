import org.junit._
import org.junit.Assert._

import latis.util.GOESUtils
import latis.util.GOESUtils.GOESGeoCalculator

class TestGOESUtils {
  // TODO: as this code is further generalized for GOES west and other GOES views, 
  // write a unit test for every function in GOESUtils
  val DELTA = 0.000001      // six decimal places
  
  @Test
  def verifyValidLocation: Unit = {
    assertTrue(GOESUtils.isTargetVisible((41.0, -109.0)))  // NW corner of Colorado
    assertFalse(GOESUtils.isTargetVisible((48.2, 16.37)))  // Vienna, Austria
    assertTrue(GOESUtils.isTargetVisible((51.51, 0.13)))   // London, England
  } 
  
  @Test
  def verifyRoundTripCalculations: Unit = {
    val calc = GOESGeoCalculator("GOES_EAST")
    
    // Quito
    val quitoIndex = calc.geoToYX((0.1807, -78.47))
    val quitoLatLon = calc.YXToGeo(quitoIndex.get)
    assertEquals("Quito Latitude", 0.1807, quitoLatLon._1, DELTA)
    assertEquals("Quito Longitude", -78.47, quitoLatLon._2, DELTA)
    
    // Colorado
    val coloradoIndex = calc.geoToYX((41.0, -109.0))
    val coloradoLatLon = calc.YXToGeo(coloradoIndex.get)
    assertEquals("Colorado Latitude", 41.0, coloradoLatLon._1, DELTA)
    assertEquals("Colorado Longitude", -109.0, coloradoLatLon._2, DELTA)
    
    // GOES Documentation Sample
    val docSampleIndex = calc.geoToYX((33.846162, -84.690932))
    val docSampleLatLon = calc.YXToGeo(docSampleIndex.get)
    assertEquals("Document Sample Latitude", 33.846162, docSampleLatLon._1, DELTA)
    assertEquals("Document Sample Longitude", -84.690932, docSampleLatLon._2, DELTA)
  }
}