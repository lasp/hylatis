

import scala.math._

import org.apache.commons.math3.analysis.polynomials.PolynomialFunction
import org.apache.commons.math3.fitting.PolynomialCurveFitter
import org.apache.commons.math3.fitting.WeightedObservedPoint
import org.geotools.referencing.CRS
import org.geotools.referencing.GeodeticCalculator

import latis.data._
import latis.model.Dataset
import latis.util.StreamUtils._


object TestGeoLocation extends App {

  /**
   * Convert a Seq of (lon,lat) pairs into (x,y) pairs with
   * orthodromic distances (along the geoid) in meters
   * from the first element with the y-axis defined as the
   * direction from the first to the second point.
   * TODO: If the path is linear, define Cartesian xs, ys.
   */
  //TODO

  /**
   * Convert a Seq of (lon,lat) pairs into (x,y) pairs with
   * orthodromic distances (along the geoid) in meters
   * from the first element with the x and y axes aligned with
   * lon and lat.
   */
  def geoToXY(lonLats: Seq[(Double, Double)]): Seq[(Double, Double)] = {
    //TODO: implicit CRS?
    val crs = CRS.decode("EPSG:4326")
    val gc = new GeodeticCalculator(crs)

    // Set the first point as the starting point: (0,0)
    lonLats.head match {
      case (lon, lat) =>
        gc.setStartingGeographicPoint(lon, lat)
    }

    // Compute x and y in meters relative to the starting point
    lonLats map {
      case (lon, lat) =>
        gc.setDestinationGeographicPoint(lon, lat)
        val d = gc.getOrthodromicDistance() //meters
        val a = gc.getAzimuth() // degrees clockwise from north
        val rad = (90.0 - a) / 180.0 * Pi
        (d * cos(rad), d * sin(rad))
    }
  }

  //XY origin (0,0) corresponds to latLon0
  def xyToGeo(xys: Seq[(Double, Double)], lonLat0: (Double, Double)): Seq[(Double, Double)] = {
    val crs = CRS.decode("EPSG:4326")
    val gc = new GeodeticCalculator(crs)
    lonLat0 match { case (lon, lat) => gc.setStartingGeographicPoint(lon, lat) }

    xys map {
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
        gc.setDirection(a, d)
        val pt = gc.getDestinationGeographicPoint
        (pt.getX, pt.getY)
    }
  }

  /*
   * assume regular cadence and const velocity, nadir, ...
   *   each step represents the same ground distance
   *   use index as abscissa
   *
   */

  // Read GPS locations for center of each slit image
  val gpsDataset: Dataset = ??? // DatasetSource.fromName("hysics_des_veg_cloud_gps").getDataset()
  //Writer().write(ds)
  val lonLats: Seq[(Double, Double)] = unsafeStreamToSeq(gpsDataset.data.streamSamples).map {
    //case Sample(_, Array(_, Real(lat), Real(lon))) => (lon, lat)
    case Sample(_, RangeData(Number(lat), Number(lon))) => (lon, lat)
  }

  //  // Local approximation: distance in degrees, dLon reduced by cos(lat0)
  //  // Use GeodeticCalculator for more precise results and distance units
  //  def toXY(lonLat: (Double, Double), lonLat0: (Double, Double)) = (lonLat, lonLat0) match {
  //    case ((lon, lat), (lon0, lat0)) => ((lon - lon0)*cos(lat0*Pi/180.0), (lat - lat0))
  //  }
  //  val xys: Seq[(Double, Double)] = lonLats.map(ll => toXY(ll, lonLats.head))

  // (x,y) positions of slit/image centers
  val xys = geoToXY(lonLats)

  // Duplicate first and last sample for sliding operation
  val xys2 = xys.head +: xys :+ xys.last
  // Compute slope from points on either side
  val slopes = xys2.sliding(3) map {
    //TODO: deal with north-south only = infinite slope
    case Seq((x1, y1), _, (x2, y2)) => (y2 - y1) / (x2 - x1)
  }

  // Fit curve to slope data to smooth it
  val degree = 3
  val fitter = PolynomialCurveFitter.create(degree)
  val points = slopes.toList.zipWithIndex map { p =>
    new WeightedObservedPoint(1, p._2, p._1)
  }
  import scala.collection.JavaConverters._
  val coeffs = fitter.fit(points.asJava)
  val pf = new PolynomialFunction(coeffs)
  val smoothed_slopes = Seq.range(0, points.length).map(pf.value(_))

  // Compute XYs for all pixels along a slit for all images
  val n = 480
  val ds = 12.33 //slit pixel size in meters, assume const height above ground
  val c = ds * (1.0 - n) / 2
  // 1: eastward, -1: westward, 0: north or southward
  //TODO: deal with N/S direction
  //TODO: assumes E/W direction doesn't change! need sliding(2)?
  val xDirection = xys.take(2) match {
    case Seq((x1, _), (x2, _)) => signum(x2 - x1)
  }
  val allXYs = (xys zip smoothed_slopes) flatMap { //for each image
    case ((x0, y0), m) =>
      (0 until n) map { i => // for each slit pixel
        val d = sqrt(m * m + 1) //const
        val x = x0 + xDirection * (ds * i + c) * m / d
        val y = y0 - xDirection * (ds * i + c) * 1.0 / d
        (x, y)
      }
  }

  // Convert XYs into Lon, Lat
  val allLonLats = xyToGeo(allXYs, lonLats.head)

  //  val pw = new PrintWriter(new File("all_lon_lat.txt" ))
  //  allLonLats.foreach(p => pw.println(s"${p._1}, ${p._2}"))
  //  pw.close()

  //  val pw2 = new PrintWriter(new File("slopes2.txt" ))
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