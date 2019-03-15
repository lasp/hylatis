package latis.util

import scala.math._

/**
 * Transform between GRS80 geodetic coordinates  and GOES index coordinates.
 * The general approach taken is to create a GOESGeoTransform object 
 * for a specified GOES satellite and a specified GOES image type.
 */
object GOESUtils {
  val rEquator = 6378137.0                  // meters, radius of earth at equator
  val rPolar = 6356752.31414                // meters, radius of earth at poles
  val H = 42164160.0                        // meters, satellite height from center of earth
  val e = 0.0818191910435                   // unitless
  val goesImageryProjection = -1.308996939  // radians, GOES-east only
 
  
  /**
   * Latitude calculated from the center of the earth.
   */
  def geocentricLat(geodeticLatitude: Double): Double = atan(tan(geodeticLatitude) * pow(rPolar, 2) / pow(rEquator, 2))
  
  /**
   * Distance from center of the earth to the point of interest on the surface of the earth.
   */
  def geocentricDistance(geocentricLatitude: Double): Double = rPolar / sqrt(1.0 - pow(e, 2) * pow(cos(geocentricLatitude), 2))
  
  /**
   * Parameters representing x,y, and z components of view angles from satellite.
   */
  def computeSx(gd: Double, gl: Double, longitude: Double) = H - gd * cos(gl) * cos(longitude - goesImageryProjection)
  def computeSy(gd: Double, gl: Double, longitude: Double) = - gd * cos(gl) * sin(longitude - goesImageryProjection)
  def computeSz(gd: Double, gl: Double) = - gd * sin(gl)
  
  /**
   * Much of the earth is not visible from either GOES spacecraft.
   * This simple inequality determines whether a target on the earth's surface is visible.
   */
  def isTargetVisible(lonLat: (Double, Double)): Boolean = {
    val (sx, sy, sz) = computeViewAngles(lonLat)
    val right = pow(sy, 2) + pow(rEquator, 2) * pow(sz, 2) / pow(rPolar, 2)
    val left = H * (H - sx)
    left > right
  }
  
  /**
   * View angles sx, sy, sz are used for several calculations.
   */
  def computeViewAngles(lonLat: (Double, Double)): (Double, Double, Double) = lonLat match {
    case (lon, lat) => 
      // first convert degrees into radians
      val latRadians = toRadians(lat)
      val lonRadians = toRadians(lon)
          
      // convert to geocentric coordinates
      val geoLat = geocentricLat(latRadians)
      val geoDist = geocentricDistance(geoLat)
          
      // compute the satellite view angles
      val sx = computeSx(geoDist, geoLat, lonRadians)
      val sy = computeSy(geoDist, geoLat, lonRadians)
      val sz = computeSz(geoDist, geoLat)
      (sx, sy, sz)
  }

  case class GOESGeoCalculator(spaceCraft: String) {
    /**
     * Transform a lon/lat position to a y/x position.
     * The y/x GOES notation can be interpreted as row/column,
     * with (0, 0) at the upper left.
     * See p.23 in https://www.goes-r.gov/users/docs/PUG-L1b-vol3.pdf
     */
    def geoToYX(lonLat: (Double, Double)): Option[(Double, Double)] = {
      if ( isTargetVisible(lonLat) ) {
        val (sx, sy, sz) = computeViewAngles(lonLat)
        val elevationAngleNS = atan(sz / sx)
        val scanningAngleEW = asin(-sy / sqrt(pow(sx, 2) + pow(sy, 2) + pow(sz, 2)))
        
        // convert to delta radians from upper left corner
        val deltaNS = 0.151844 + elevationAngleNS
        val deltaEW = 0.151844 + scanningAngleEW
        
        // convert to index space
        val indexNS = deltaNS / 0.000056      // 0.000056 radians / index
        val indexEW = deltaEW / 0.000056
        
        println("NS: " + elevationAngleNS + ", EW: " + scanningAngleEW)
        println("   NS index: " + indexNS + ", EW index: " + indexEW)
        Some(indexNS, indexEW)
      } else {
        None
      }
    }
    
    /**
     * Transform a y/x position to a lon/lat position.
     * The y/x GOES notation can be interpreted as row/column,
     * with (0, 0) at the upper left.
     * 
     * See p.21 in https://www.goes-r.gov/users/docs/PUG-L1b-vol3.pdf
     */
    def YXToGeo(yx: (Double, Double)): (Double, Double) = yx match {
      case (y, x) => {
        // TODO: implement after experience is gained running geoToYX  
      }
      (0.0, 0.0)
    }
  }
}
