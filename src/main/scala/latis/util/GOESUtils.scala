package latis.util

import scala.math._

import latis.data.Data
import latis.data.DomainData
import latis.data.Number
import latis.data.TupleData
import latis.dataset.ComputationalDataset
import latis.metadata.Metadata
import latis.model.Function
import latis.model.Scalar
import latis.model.Tuple
 
/**
 * Transform between GRS80 geodetic coordinates and GOES index coordinates.
 * The general approach taken is to create a GOESGeoTransform object 
 * for a specified GOES satellite and a specified GOES image type.
 * 
 * The point P refers to a point on the earth' surface.
 * 
 * Parameters sx, sy, and sz represent x,y, and z components of view vectors from satellite to point P.
 * 
 * The current implementation is for GOES-East only.
 */
object GOESUtils {
  val rEquator = 6378137.0                  // meters, radius of earth at equator
  val rPolar = 6356752.31414                // meters, radius of earth at poles
  val H = 42164160.0                        // meters, satellite height from center of earth
  val e = 0.0818191910435                   // unitless
  val goesImageryProjection = -1.308996939  // radians, GOES-east only
  
  val radiansPerPixel = 0.000056            // radians per pixel in full disk images
  val imageOffsetNS = 0.151844              // radians upper left corner offset from image center on y axis
  val imageOffsetEW = 0.151844              // radians upper left corner offset from image center on x axis
          
  val scaleFactor: Int = LatisConfig.getOrElse("hylatis.goes.scale-factor", 1)
  
  /**
   * Latitude of point P calculated from the center of the earth.
   * @param geodeticLatitude GRS80 or map latitude
   * @return geocentric latitude relative to the center of the earth
   */
  def geocentricLat(geodeticLatitude: Double): Double = atan(tan(geodeticLatitude) * pow(rPolar, 2) / pow(rEquator, 2))
  
  /**
   * Distance from center of the earth to the point of interest (P) on the surface of the earth.
   * Units are meters.
   * @param geocentricLatitude latitude relative to the center of the earth
   * @return meters
   */
  def geocentricDistance(geocentricLatitude: Double): Double = rPolar / sqrt(1.0 - pow(e, 2) * pow(cos(geocentricLatitude), 2))
  
  /**
   * Distance from satellite to the point of interest (P) on the surface of the earth.
   * @param y N/S angle in radians in fixed-grid coordinates
   * @param x E/W angle in radians in fixed-grid coordinates
   * @return length of sx, sy, sy vector in meters
   */
  def satelliteDistance(yx: (Double, Double)): Double = yx match {
    case (y, x) => {
      val a = pow(sin(x), 2) + pow(cos(x), 2) * (pow(cos(y), 2) + pow(rEquator, 2) / pow(rPolar, 2) * pow(sin(y),2))
      val b = -2.0 * H * cos(x) * cos(y)
      val c = pow(H, 2) - pow(rEquator, 2)
      
      (-b - sqrt(pow(b, 2) - 4 * a * c)) / (2 * a)
    }
  }
  
  /**
   * Much of the earth is not visible from either GOES spacecraft.
   * This simple inequality determines whether a target on the earth's surface is visible.
   * @param lat latitude in GRS80 geodetic coordinates
   * @param lon longitude in GRS80 geodetic coordinates
   * @return true if point P is visible by GOES satellite
   */
  def isTargetVisible(latLon: (Double, Double)): Boolean = {
    val (sx, sy, sz) = computeViewVectorsfromLatLon(latLon)
    val right = pow(sy, 2) + pow(rEquator, 2) * pow(sz, 2) / pow(rPolar, 2)
    val left = H * (H - sx)
    left > right
  }
  
  /**
   * View vector sx, sy, sz points from satellite to point P on earth's surface.
   * @param y N/S angle in radians in fixed-grid coordinates
   * @param x E/W angle in radians in fixed-grid coordinates
   * @return vector from GOES satellite to P
   */
  def computeViewVectorsFromFixedGrid(yx: (Double, Double), sd: Double) : (Double, Double, Double) = yx match {
    case (y, x) =>
      val sx = sd * cos(x) * cos(y)
      val sy = -sd * sin(x)
      val sz = sd * cos(x) * sin(y)
      (sx, sy, sz)
  }
  
  /**
   * Compute vector sx, sy, sz which points from satellite to point P on earth's surface.
   * @param lon longitude in GRS80 geodetic coordinates
   * @param lat latitude in GRS80 geodetic coordinates
   * @return vector from GOES satellite to P
   */
  def computeViewVectorsfromLatLon(latLon: (Double, Double)): (Double, Double, Double) = latLon match {
    case (lat, lon) => 
      // first convert degrees into radians
      val latRadians = toRadians(lat)
      val lonRadians = toRadians(lon)
          
      // convert to geocentric coordinates
      val geoLat = geocentricLat(latRadians)
      val geoDist = geocentricDistance(geoLat)
          
      // compute the satellite view vectors
      val sx =  H - geoDist * cos(geoLat) * cos(lonRadians - goesImageryProjection)     
      val sy =  -geoDist * cos(geoLat) * sin(lonRadians - goesImageryProjection)
      val sz = geoDist * sin(geoLat) 
      
      (sx, sy, sz)
  }
  
  /**
   * Convert from index space to radians.
   * (0, 0) is the upper left corner.
   * (5424, 5424) is the lower left corner.
   * @param y N/S angle in index value between 0 and 5424
   * @param x E/W angle in index value between 0 and 5424
   * @return tuple of point P in radians for fixed-grid coordinates
   */
  def indexToRadians(yx: (Double, Double)): (Double, Double) = yx match {
    case (y, x) =>
      val yRadians = imageOffsetNS - y * radiansPerPixel
      val xRadians = -imageOffsetEW + x * radiansPerPixel
      
      (yRadians, xRadians)
  }
  
  /** Convert from radians to index space.
   * (0, 0) is the upper left corner.
   * (5424, 5424) is the lower left corner.
   * @param y N/S angle in radians in fixed-grid coordinates
   * @param x E/W angle in radians in fixed-grid coordinates 
   * @return tuple of point P in index value between 0 and 5424 for y and x
   */
  def radiansToIndex(yx: (Double, Double)): (Double, Double) = yx match {
    case (y, x) =>
      val yIndex = (imageOffsetNS - y) / radiansPerPixel / scaleFactor
      val xIndex = (imageOffsetEW + x) / radiansPerPixel / scaleFactor
      
      (yIndex, xIndex)
  }

  def xyToGeo(xy: (Double, Double)): (Double, Double) = xy match {
    case (x, y) => {
      //val radLocation = indexToRadians(yIndex, xIndex)
      val yx = (y, x)
      val satDist = satelliteDistance(yx)
      val (sx, sy, sz) = computeViewVectorsFromFixedGrid((yx), satDist)
      val geoLat = atan(pow(rEquator/rPolar, 2) * sz / sqrt(pow(H - sx, 2) + pow(sy, 2)))
      val geoLon = goesImageryProjection - atan(sy / (H - sx))
      //(toDegrees(geoLat), toDegrees(geoLon))
      (toDegrees(geoLon), toDegrees(geoLat))
    }
  }

  case class GOESGeoCalculator(spaceCraft: String) {
    /**
     * Transform a lat/lon position to a y/x position.
     * The y/x GOES notation can be interpreted as row/column,
     * with (0, 0) at the upper left. 
   	 * @param lat latitude in GRS80 geodetic coordinates 
     * @param lon longitude in GRS80 geodetic coordinates
   	 * @return option of tuple of point P in index value between 0 and 5424 for y and x
     * 
     * See p.23 in https://www.goes-r.gov/users/docs/PUG-L1b-vol3.pdf
     */
    def geoToYX(latLon: (Double, Double)): Option[(Double, Double)] = {
      if ( isTargetVisible(latLon) ) {
        val (sx, sy, sz) = computeViewVectorsfromLatLon(latLon)
        val elevationAngleNS = atan(sz / sx)
        val scanningAngleEW = asin(-sy / sqrt(pow(sx, 2) + pow(sy, 2) + pow(sz, 2)))
        Some(radiansToIndex(elevationAngleNS, scanningAngleEW))
      } else {
        None
      }
    }
    
    /**
     * Transform a y/x position to a lon/lat position.
     * The y/x GOES notation can be interpreted as row/column,
     * with (0, 0) at the upper left.
     * @param y N/S angle in index value between 0 and 5424
     * @param x E/W angle in index value between 0 and 5424
     * @return option of tuple of point P in latitude and longitude
     *  
     * See p.21 in https://www.goes-r.gov/users/docs/PUG-L1b-vol3.pdf
     */
    def YXToGeo(yxIndices: (Double, Double)): (Double, Double) = yxIndices match {
      case (yIndex, xIndex) => {
        val radLocation = indexToRadians(yIndex, xIndex)
        val satDist = satelliteDistance(radLocation)
        val (sx, sy, sz) = computeViewVectorsFromFixedGrid((radLocation), satDist)
        val geoLat = atan(pow(rEquator/rPolar, 2) * sz / sqrt(pow(H - sx, 2) + pow(sy, 2)))
        val geoLon = goesImageryProjection - atan(sy / (H - sx))
        (toDegrees(geoLat), toDegrees(geoLon))
      }
    }
  }

  val geoCSX: ComputationalDataset = {
    val md = Metadata("goes_geo_csx")
    val model = Function(
      Tuple(
        Scalar(Metadata("x") + ("type" -> "double")),
        Scalar(Metadata("y") + ("type" -> "double"))
      ),
      Tuple(
        Scalar(Metadata("lon") + ("type" -> "double")),
        Scalar(Metadata("lat") + ("type" -> "double"))
      )
    )

    val csx: ((Double, Double)) => (Double, Double) =
      GOESUtils.xyToGeo

    val f: Data => Either[LatisException, Data] = {
      (data: Data) => data match {
        case TupleData(Number(x), Number(y)) =>
          csx((x, y)) match {
            case (lon, lat) =>
              //if (lon.isNaN || lat.isNaN) {
              //  Left(LatisException("Invalid coordinate"))
              //} else {
              val dd = DomainData(lon, lat)
              Right(TupleData(dd))
            //}
          }
        case _ => Left(LatisException(s"Function expected x-y pair but got $data"))
      }
    }

    ComputationalDataset(md, model, f)
  }
}
