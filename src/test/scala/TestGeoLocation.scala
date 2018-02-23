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


object TestGeoLocation extends App {
  
  /*
   * assume regular cadence and const velocity, nadir, ...
   *   each step represents the same ground distance
   *   use index as abscissa 
   * 
   */
  
  // Read GPS locations for center of each slit image
  val ds = DatasetSource.fromName("hysics_des_veg_cloud_gps").getDataset()
  //Writer().write(ds)
  val lonLats: Seq[(Double, Double)] = ds.samples.toSeq.map {
    case Sample(time, TupleData(Seq(Real(lat), Real(lon)))) => (lon,lat)
  }
  
  // Local approximation: distance in degrees, dLon reduced by cos(lat0)
  // Use GeodeticCalculator for more precise results and distance units
  import scala.math._
  def toXY(lonLat: (Double, Double), lonLat0: (Double, Double)) = (lonLat, lonLat0) match {
    case ((lon, lat), (lon0, lat0)) => ((lon - lon0)*cos(lat0*Pi/180.0), (lat - lat0))
  }
  val xys: Seq[(Double, Double)] = lonLats.map(ll => toXY(ll, lonLats.head))
  
  //xys foreach println
//  val pw = new PrintWriter(new File("xys.txt" ))
//  xys.foreach(p => pw.println(s"${p._1}, ${p._2}"))
//  pw.close()
  
  // Duplicate first and last sample for sliding operation
  val xys2 = xys.head +: xys :+ xys.last
  // Compute slope from points on either side
  val slopes = xys2.sliding(3) map {
    case Seq((x1, y1), _, (x2, y2)) => (y2 - y1)/(x2 - x1) //TODO: deal with north-south only = infinite slope
  }
  
  // Fit curve to slope data to smooth it
  val degree = 3
  val fitter = PolynomialCurveFitter.create(degree)
  val points = slopes.toList.zipWithIndex map { p =>
    new WeightedObservedPoint(1, p._2, p._1)
  }
  import collection.JavaConverters._
  val coeffs = fitter.fit(points.asJava)
  val pf = new PolynomialFunction(coeffs)
  val smoothed_slopes = Seq.range(0, points.length).map(pf.value(_))
  
//  val pw2 = new PrintWriter(new File("slopes_smoothed.txt" ))
//  smoothed_slopes foreach pw2.println
//  pw2.close()
  
  /*
   * there are duplicate lon values in the gps data 
   * spline interp requires strictly increasing
   * maybe we do need to use a diff coord system?
   *   even x won't work for some trajectories
   * or localized fit?
   * can we smooth first?
   * 
   * do sliding window of 3 and compute slope from neighbors
   * repeat slope at endpoints?
   *   probably should just use the 2 points at the ends
   *  *just repeat the first and last samples before "sliding"?
   *   define set of end-point behaviors for sliding operations
   * 
   * Use parametric curve: x(i), y(i)
   *   dy/dx = (dy/di) / (dx/di))
   *   the inner two will be strictly increasing
   *   the spline seems to be to true to the data, hits every point, we need to smooth
   *   
   * TODO: try PolynomialCurveFitter.create(3).fit(obs)
   *   and make Polynomial function from coeffs
   *   which has a derivative
   *   or just fit the slopes
   *   
   * Consider general trajectory and overlap of slit images
   *   
   */
  
  
  /*
   * Calculate distance between each image lon,lat location.
   * https://www.nhc.noaa.gov/gccalc.shtml
   *  
   * Assume equal spacing of images? avg then use a dy
   * But assumes linear track.
   */
//  val crs = CRS.decode("EPSG:4326")
//  val gc = new GeodeticCalculator(crs)
//  //val p1 = new Coordinate(3.4714011e+01,  -1.0821368e+02)
//  //val p2 = new Coordinate(3.4714015e+01,  -1.0821369e+02)
//  val p1 = new Coordinate(34.714011, -108.21368) //lat,lon
//  val p2 = new Coordinate(34.731971, -108.2499)
//  gc.setStartingPosition( JTS.toDirectPosition( p1, crs ) )
//  gc.setDestinationPosition( JTS.toDirectPosition( p2, crs ) )
//    
//  val d = gc.getOrthodromicDistance() //meters
//  //val d = JTS.orthodromicDistance(p1, p2, crs)
//  println(d)
  
}