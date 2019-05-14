package latis.ops

import latis.data._
import latis.model._
import latis.util.GOESUtils._

import scala.math._

/**
 * Goes-specific version of BoundingBoxEvaluation class.
 * Given the minimum and maximum coordinates for a 2-dimensional rectangular domain
 * define a regular grid and evaluate (resample) the input Dataset onto that grid.
 * Include the minimum edges but exclude the maximum edges so contiguous bounding
 * box requests won't overlap. This is consistent with capturing the grid cell
 * centers with a half cell offset.
 */
class GoesGridEvaluation(lon1: Double, lat1: Double, lon2: Double, lat2: Double, nx: Int, ny: Int) extends MapOperation {
  //val calc = GOESGeoCalculator("GOES_EAST")
  
  /**
   * Define the target domain set.
   */
  lazy val grid: DomainSet = {
    val dx = (lon2 - lon1) / nx
    val dy = (lat2 - lat1) / ny
    val dds = for {
      lon <- (0 until nx).map(lon1 + dx * _)
      lat <- (0 until ny).map(lat1 + dy * _)
      //latLon <- calc.geoToYX((lat, lon))
    } yield DomainData(lon, lat)
    //yield DomainData(round(latLon._1).toInt, round(latLon._2).toInt)
    DomainSet(dds)
  }
  
//  /**
//   * Gather the data into a local GoesArrayFunction2D and apply the resampling.
//   */
//  override def applyToData(data: SampledFunction, model: DataType): SampledFunction = {
//    GoesArrayFunction2D.restructure(data)(grid)
//  }

  // evaluate the range of each sample
  def makeMapFunction(model: DataType): Sample => Sample = (sample: Sample) =>
    sample match {
      case Sample(domain, RangeData(sf: SampledFunction)) =>
        Sample(domain, RangeData(sf(grid)))
    }
  
  
  // Replace domain in nested function
  override def applyToModel(model: DataType): DataType = model match {
    case Function(domain, Function(_, range)) => Function(
      domain, Function(Tuple(Scalar("longitude"), Scalar("latitude")), range)
    )
  }
}

object GoesGridEvaluation {
  
  def apply(x1: Double, y1: Double, x2: Double, y2: Double, nx: Int, ny: Int): GoesGridEvaluation =
    new GoesGridEvaluation(x1, y1, x2, y2, nx, ny)
  
  /**
   * Convenience constructor for BoundingBoxEvaluation given min and max corners
   * and desired total cell count.
   */
  def apply(x1: Double, y1: Double, x2: Double, y2: Double, n: Int): GoesGridEvaluation = {
    val dx = x2 - x1
    val dy = y2 - y1
    val nx: Int = Math.round(Math.sqrt(dx * n / dy)).toInt
    val ny: Int = Math.round(n.toFloat / nx)
    GoesGridEvaluation(x1, y1, x2, y2, nx, ny)
  }
}
