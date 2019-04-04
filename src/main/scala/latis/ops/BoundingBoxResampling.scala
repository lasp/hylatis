package latis.ops

import latis.data._
import latis.model.DataType

/**
 * Given the minimum and maximum coordinates for a 2-dimensional rectangular domain
 * define a regular grid and evaluate (resample) the input Dataset onto that grid.
 * Include the minimum edged but exclude the maximum edges so contiguous bounding
 * box requests won't overlap. This is consistent with capturing the grid cell
 * centers with a half cell offset.
 */
class BoundingBoxResampling(x1: Double, y1: Double, x2: Double, y2: Double, nx: Int, ny: Int) extends UnaryOperation {
  //TODO: assert that x1 < x2 and y1 < y2, or get more clever about ordering
  //TODO: generalize to two nD points, shape
  //TODO: add bin/cell semantics

  /**
   * Define the target domain set.
   */
  lazy val grid: Seq[DomainData] = {
    val dx = (x2 - x1) / nx
    val dy = (y2 - y1) / ny
    for {
      x <- (0 until nx).map(x1 + dx * _)
      y <- (0 until ny).map(y1 + dy * _)
    } yield DomainData(x, y)
  }
  
  override def applyToData(data: SampledFunction, model: DataType): MemoizedFunction = {
    //TODO: define DomainSet then apply it to SampledFunction
    val samples = grid.map( dd => Sample(dd, data(dd).get))
    //TODO: make IndexedFunction2D
    SampledFunction.fromSeq(samples)
  }
}

object BoundingBoxResampling {
  
  def apply(x1: Double, y1: Double, x2: Double, y2: Double, nx: Int, ny: Int): BoundingBoxResampling =
    new BoundingBoxResampling(x1, y1, x2, y2, nx, ny)
  
  /**
   * Convenience constructor for BoundingBoxResampling given min and max corners
   * and desired total cell count.
   */
  def apply(x1: Double, y1: Double, x2: Double, y2: Double, n: Int): BoundingBoxResampling = {
    val dx = x2 - x1
    val dy = y2 - y1
    val nx: Int = Math.round(Math.sqrt(dx * n / dy)).toInt
    val ny: Int = Math.round(n.toFloat / nx)
    BoundingBoxResampling(x1, y1, x2, y2, nx, ny)
  }
}