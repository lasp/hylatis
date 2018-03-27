package latis.util

import scala.math._
import org.geotools.referencing.CRS
import org.geotools.referencing.GeodeticCalculator

//So far, just des_veg_cloud geo ref stuff
object HysicsUtils {
  
  //TODO: consider life cycle of mutable geo calc; not thread safe
  val geoCalc = {
    val crs = CRS.decode("EPSG:4326")
    val gc = new GeodeticCalculator(crs)
    gc.setStartingGeographicPoint(-108.21367912062485, 34.714010512782387)
    gc
  }

  //val nx = 480
  //val ny = 4200
  val a = -59.00 * Pi / 180.0  //altitude
  val dx = 12.358              //step size along slit
  val dy = 0.92167             //step size between images
  
  def x(i: Int): Double = dx * (i - 239.5) //center on slit
  def y(j: Int): Double = dy * j
  
  val indexToXY = (ij: (Int,Int)) => (x(ij._1), y(ij._2))
  
  // a in radians
  val rotateXY = (xy: (Double, Double)) => {
    val x = xy._1
    val y = xy._2
    val x2 =   x * cos(a) + y * sin(a)
    val y2 = - x * sin(a) + y * cos(a)
    (x2, y2)
  }
  
  // x,y aligned with lon,lat
  val xyToGeo = (xy: (Double, Double)) => {
    val x = xy._1
    val y = xy._2
    
    //azimuth (a): degrees from north: -180 to 180
    //arctan : -90 to 90
    //Need to use sign of x to get sign right
    val a = signum(x) match {
      case 0 =>
        if (y > 0) 0.0
        else 180.0
      case sign => 90.0 * sign - atan(y/x) * 180.0 / Pi
    }
    val d = sqrt(x*x + y*y)
    geoCalc.setDirection(a, d)
    val pt = geoCalc.getDestinationGeographicPoint
    (pt.getX, pt.getY)
  }
  
  
  val indexToGeo = (indexToXY andThen rotateXY andThen xyToGeo)
}