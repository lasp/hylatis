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


object TestGeoLocation extends App {
  
  /*
   * dx = 13.16 m/px along slit
   * 
   */
  
  val ds = DatasetSource.fromName("hysics_des_veg_cloud_gps").getDataset()
  //Writer().write(ds)
  val lonLats: Seq[(Double, Double)] = ds.samples.toSeq.map {
    case Sample(time, TupleData(Seq(Real(lat), Real(lon)))) => (lon,lat)
  }
  
  
  // Local approximation: distance in degrees, dLon stretched by 1/cos(lat0)
  import scala.math._
  def toXY(lonLat: (Double, Double), lonLat0: (Double, Double)) = (lonLat, lonLat0) match {
    case ((lon, lat), (lon0, lat0)) => ((lon - lon0)/cos(lat0*Pi/180.0), (lat - lat0))
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
    case Seq((x1, y1), (x, _), (x2, y2)) => (x, (y2 - y1)/(x2 - x1))
  }
  
  
  //only works if strictly increasing
//  val (rxs, rys) = xys.reverse.toArray.unzip
//  val spline = new SplineInterpolator().interpolate(rxs, rys)
//  val deriv = spline.derivative()
//  val slopes = rxs.reverse.map(x => (x, deriv.value(x) * (-1))) //re-reverse xs, negate slope
  
//  val (xs, ys) = xys.toArray.unzip
//  val is = Array.range(0, xs.length).map(_.toDouble)
//  val xspline = new LoessInterpolator().interpolate(is, xs)
//  val xderiv = xspline.derivative()
//  val xslopes = is.map(xderiv.value(_))
  
  val pw = new PrintWriter(new File("slopes_spline.txt" ))
  slopes.foreach (p => pw.println(s"${p._1}, ${p._2}"))
  pw.close()
  
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
   *   AkimaSplineInterpolator, got const slope -1.21654e-05
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