package latis.ops

import latis.data._
import latis.model.DataType
import latis.util.GOESUtils
import latis.util.GOESUtils.GOESGeoCalculator
import latis.util.LatisProperties

/**
 * Given the minimum (south-west) and maximum (north-east) coordinates for a 
 * longitude-latitude rectangular domain, apply selections to filter out all
 * samples but the ones that fall inside (inclusive on the minimum edges, 
 * exclusive on the max edges) the box. 
 * Longitude values range from -180 up to 180 degrees.
 * Latitude values range from -90 to 90 degrees.
 * This may use a coordinate system transformation to convert geo 
 * coordinates to native spatial coordinates.
 * TODO: comment on default geoid
 */
class GeoBoundingBox(lon1: Double, lat1: Double, lon2: Double, lat2: Double) extends Filter {
  //TODO: check bounds, order, apply modulus to longitude
  //TODO: add bin/cell semantics
  //TODO: enable coordinate system transformation injection

  // Hard-wired for GOES ABI grid transformation
  val calc = GOESGeoCalculator("")
  var (y2, x1) = calc.geoToYX((lat1, lon1)).get //TODO: orElse error
  var (y1, x2) = calc.geoToYX((lat2, lon2)).get //TODO: orElse error
  val scale: Int = LatisProperties.get("goes.scale.factor").map(_.toInt).getOrElse(1)
  x1 = Math.round(x1 / scale)
  x2 = Math.round(x2 / scale)
  y1 = Math.round(y1 / scale)
  y2 = Math.round(y2 / scale)

  def makePredicate(model: DataType): Sample => Boolean = {
    val p1 = Selection(s"ix >= $x1").makePredicate(model)
    val p2 = Selection(s"iy >= $y1").makePredicate(model)
    val p3 = Selection(s"ix < $x2").makePredicate(model)
    val p4 = Selection(s"iy < $y2").makePredicate(model)
    (s: Sample) => p1(s) && p2(s) && p3(s) && p4(s)
    //TODO: fold map cleverness
  }
  
}


object GeoBoundingBox {
  
  def apply(lon1: Double, lat1: Double, lon2: Double, lat2: Double): GeoBoundingBox =
    new GeoBoundingBox(lon1, lat1, lon2, lat2)
  
}