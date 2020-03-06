package latis.util

import scala.math._

import org.geotools.referencing.CRS
import org.geotools.referencing.GeodeticCalculator
import os./

import latis.data.Data
import latis.data.Index
import latis.data.Number
import latis.data.TupleData
import latis.dataset.ComputationalDataset
import latis.metadata.Metadata
import latis.model.Function
import latis.model.Scalar
import latis.model.Tuple

//So far, just des_veg_cloud geo ref stuff
object HysicsUtils {
  /*
  TODO: update coord system:
    x: along trajectory (originally y)
    y: along scan such that x cross y is up (originally x, but not sure about order
    the x,y defs have been updated but indexToXY on down needs to be avaluated
   */
  
  //TODO: consider life cycle of mutable geo calc; not thread safe
  lazy val geoCalc = {
    val crs = CRS.decode("EPSG:4326")
    val gc = new GeodeticCalculator(crs)
    gc.setStartingGeographicPoint(-108.21367912062485, 34.714010512782387)
    gc
  }

  //val ny = 480
  //val nx = 4200
  /**
   * Azimuth (radians clockwise from north) of flight path based on linear assumption
   * and GeodeticCalculator. (HYLATIS-35)
   */
  val azimuth = -59.00 * Pi / 180.0

  /**
   * Defines the angle (counter-clockwise) that the original x-y coordinate system
   * must be rotated to align with the lon-lat coordinate system.
   */
  val angle = Pi / 2.0 - azimuth


  /**
   * Step size in meters of pixels along the slit (y). Linear approximation.
   */
  val dy = 12.358
  
  /**
   * Step size in meters of slit images along the flight path direction (x).
   * Linear approximation.
   */
  val dx = 0.92167
  
  /**
   * Compute y value of the ith pixel center with origin at the slit center.
   * Paul Smith believes that the slit scan direction is left to right
   * relative to the x-direction. This is the reverse of our swath
   * coordinate system.
   * TODO: does this cause other ordering issues?
   */
  //def y(i: Int): Double = dy * (i - 239.5) //center on slit, ny = 480
  def y(i: Int): Double = dy * (239.5 - i) //center on slit, ny = 480
  
  /**
   * Compute x value of the jth pixel center.
   */
  def x(j: Int): Double = dx * j
  
  /**
   * Function to transform spatial indices to x/y coordinates.
   */
  val indexToXY: ((Int, Int)) => (Double, Double) = (ij: (Int,Int)) => (x(ij._1), y(ij._2))
  //val indexToXY: (Int, Int) => (Double, Double) = (i: Int, j: Int) => (x(i), y(j))  //can't compose
  
  /**
   * Make a function to transform a spatial x/y coordinate to an x/y coordinate system
   * that is rotated about the origin by the angle "a" (in radians).
   */
  def rotateXY(a: Double): ((Double, Double)) => (Double, Double) = {
    val cosa = cos(a)
    val sina = sin(a)
    
    (xy: (Double, Double)) => xy match {
      case (x, y) => (x * cosa - y * sina, x * sina + y * cosa)
    }
  }

  /**
   * Transform a x/y position to a lon/lat position.
   * This assumes the x/y grid is aligned with lon/lat.
   */
  val xyToGeo: ((Double, Double)) => (Double, Double) = (xy: (Double, Double)) => xy match {
    case (x, y) =>
      //azimuth (a): degrees from north: -180 to 180
      //arctan : -90 to 90
      //Need to use sign of x to get sign right
      //val a = signum(x) match {
      //  case 0 =>
      //    if (y > 0) 0.0
      //    else 180.0
      //  //case sign => 90.0 * sign - atan(y / x) * 180.0 / Pi
      //  case sign => sign *  atan(x / y) * 180.0 / Pi
      //}
      val arctan = atan(x / y) * 180.0 / Pi  // -90 to 90
      val a: Double = if (y >= 0) arctan
        else arctan + 180.0 * signum(x)
      val d = sqrt(x * x + y * y)
      geoCalc.setDirection(a, d)
      val pt = geoCalc.getDestinationGeographicPoint
      (pt.getX, pt.getY)
  }
  
  /**
   * Rotate Hysics x/y grid to align with lon/lat grid.
   * The x-direction is the direction of the flight.
   * The original x-y grid has x pointing east.
   * We need to rotate 90 - azimuth.
   */
  val hysicsToGeo = rotateXY(angle) andThen xyToGeo

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
  
  val geoToHysics = geoToXY andThen rotateXY(-1 * angle)
  
  //TODO: be clear about XY coordinate system, orig vs rotated, see usage in HysicsLocalReader
  //TODO: geoToXY = (lonLat: (Double, Double)) => {
  
  val indexToGeo = (indexToXY andThen hysicsToGeo)


  /**
   * Defines the coordinate system transform from the index coordinate system
   * to the spatial x-y coordinate system.
   */
  val xyCSX: ComputationalDataset = {
    val md = Metadata("hysics_xy_csx")
    val model = Function(
      Tuple(
        Scalar(Metadata("ix") + ("type" -> "int")),
        Scalar(Metadata("iy") + ("type" -> "int"))
      ),
      Tuple(
        Scalar(Metadata("x") + ("type" -> "double")),
        Scalar(Metadata("y") + ("type" -> "double"))
      )
    )

    val f: Data => Either[LatisException, Data] = {
      (data: Data) => data match {
        case TupleData(Index(ix), Index(iy)) =>
          val td = TupleData(
            HysicsUtils.x(ix),
            HysicsUtils.y(iy)
          )
          Right(td)
      }
    }

    ComputationalDataset(md, model, f)
  }

  /**
   * Defines the coordinate system transform from x-y to
   * lon-lat geo coordinates.
   */
  val geoCSX: ComputationalDataset = {
    val md = Metadata("hysics_geo_csx")
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

    val f: Data => Either[LatisException, Data] = {
      (data: Data) => data match {
        case TupleData(Number(x), Number(y)) =>
          HysicsUtils.hysicsToGeo((x, y)) match {
            case (lon, lat) =>
              Right(TupleData(lon, lat))
          }
      }
    }

    ComputationalDataset(md, model, f)
  }
}
