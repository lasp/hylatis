package latis.util

import scala.math._
import org.geotools.referencing.CRS
import org.geotools.referencing.GeodeticCalculator

//So far, just des_veg_cloud geo ref stuff
object HysicsUtils {
  
  //TODO: consider life cycle of mutable geo calc; not thread safe
  lazy val geoCalc = {
    val crs = CRS.decode("EPSG:4326")
    val gc = new GeodeticCalculator(crs)
    gc.setStartingGeographicPoint(-108.21367912062485, 34.714010512782387)
    gc
  }

  //val nx = 480
  //val ny = 4200
  /**
   * Azimuth (radians clockwise from north) of flight path based on linear assumption
   * and GeodeticCalculator. (HYLATIS-35)
   */
  val azimuth = -59.00 * Pi / 180.0
  
  /**
   * Step size in meters of pixels along the slit (x). Linear approximation.
   */
  val dx = 12.358
  
  /**
   * Step size in meters of slit images along the flight path direction (y).
   * Linear approximation.
   */
  val dy = 0.92167
  
  /**
   * Compute x value of the ith pixel center with origin at the slit center.
   */
  def x(i: Int): Double = dx * (i - 239.5) //center on slit, nx = 480
  
  /**
   * Compute y value of the jth pixel center.
   */
  def y(j: Int): Double = dy * j
  
  /**
   * Function to transform spatial indices to x/y coordinates.
   */
  val indexToXY = (ij: (Int,Int)) => (x(ij._1), y(ij._2))
  
  /**
   * Make a function to transform a spatial x/y coordinate to an x/y coordinate system
   * that is rotated about the origin by the angle "a" (in radians).
   * TODO: sanity check signs
   */
  def rotateXY(a: Double) = {
    val cosa = cos(a)
    val sina = sin(a)
    
    (xy: (Double, Double)) => xy match {
      case (x, y) => (x * cosa + y * sina, - x * sina + y * cosa)
    }
  }

  /**
   * Transform a x/y position to a lon/lat position.
   * This assumes the x/y grid is aligned with lon/lat.
   */
  val xyToGeo = (xy: (Double, Double)) => xy match {
    case (x, y) =>
      //azimuth (a): degrees from north: -180 to 180
      //arctan : -90 to 90
      //Need to use sign of x to get sign right
      val a = signum(x) match {
        case 0 =>
          if (y > 0) 0.0
          else 180.0
        case sign => 90.0 * sign - atan(y / x) * 180.0 / Pi
      }
      val d = sqrt(x * x + y * y)
      geoCalc.setDirection(a, d)
      val pt = geoCalc.getDestinationGeographicPoint
      (pt.getX, pt.getY)
  }
  
  /**
   * Rotate Hysics x/y grid to align with lon/lat grid.
   * The rotation angle (counter-clockwise) is the same as the azimuth of the path (non-intuitively).
   */
  val hysicsToGeo = rotateXY(azimuth) andThen xyToGeo

  /**
   * Transform a lon/lat position to a x/y position.
   * This assumes the x/y grid is aligned with lon/lat.
   */
  val geoToXY = (lonLat: (Double, Double)) => lonLat match {
    case (lon, lat) =>
      geoCalc.setDestinationGeographicPoint(lon, lat)
      val d = geoCalc.getOrthodromicDistance() //meters
      val a = geoCalc.getAzimuth() // degrees clockwise from north
      val rad = (90.0 - a) / 180.0 * Pi
      (d * cos(rad), d * sin(rad))
  }
  
  val geoToHysics = geoToXY andThen rotateXY(-1 * azimuth)
  
  //TODO: be clear about XY coordinate system, orig vs rotated, see usage in HysicsLocalReader
  //TODO: geoToXY = (lonLat: (Double, Double)) => {
  
  val indexToGeo = (indexToXY andThen hysicsToGeo)
}