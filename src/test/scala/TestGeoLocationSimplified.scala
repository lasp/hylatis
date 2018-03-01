import org.geotools.referencing.CRS
import org.geotools.referencing.GeodeticCalculator
import org.geotools.geometry.jts.JTS
import com.vividsolutions.jts.geom.Coordinate
import latis.reader.DatasetSource
import latis.writer.Writer
import latis.data._
import scala.collection.mutable.ArrayBuffer
import latis.model.Real
import java.io._
import org.apache.commons.math3.analysis.interpolation._
import org.apache.commons.math3.fitting.PolynomialCurveFitter
import org.apache.commons.math3.fitting.WeightedObservedPoint
import org.apache.commons.math3.analysis.polynomials.PolynomialFunction
import scala.math._

object TestGeoLocationSimplified extends App {
  
  val crs = CRS.decode("EPSG:4326")
  val geoCalc = new GeodeticCalculator(crs)
  geoCalc.setStartingGeographicPoint(-108.21367912062485, 34.714010512782387)
  
  val nx = 480
  val ny = 4200
  val a = -59.00 * Pi / 180.0
  val dx = 12.358
  val dy = 0.92167
  
  def x(i: Int): Double = dx * (i - 239.5)
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
  
  
  val f = (indexToXY andThen rotateXY andThen xyToGeo)
  val allLonLats = for (j <- 0 until ny; i <- 0 until nx) yield f((i,j))
      
  val pw = new PrintWriter(new File("all_lon_lat3.txt" ))
  allLonLats.foreach(p => pw.println(s"${p._1}, ${p._2}"))
  pw.close()
  
  /*
     * Note, unlike TestGeoLocation, this XY cs is rotated relative to lon,lat.
     * Since our XY cs has (0,0) at the StartingGeographicPoint, we can simply rotate
     * the XY grid to a X'Y' grid that aligns with geo
     */
  
}