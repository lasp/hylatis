package latis.data

import latis.resample._

/**
 * Define a SampledFunction whose domain is defined
 * in terms of scale and offset.
 * This assumes that the values array is Cartesian:
 * same number of values in each internal array
 */
case class LinearFunction2D(
  xScale: Double, xOffset: Double, 
  yScale: Double, yOffset: Double, 
  values: Array[Array[Any]]
) extends MemoizedFunction {
  
  val nx = values.length
  val ny = values(0).length
  
  /**
   * Provide a Seq of Samples with domain values computed
   * from scale and offset.
   */
  def samples: Seq[Sample] = for {
      ix <- (0 until nx)
      iy <- (0 until ny)
    } yield Sample(
      DomainData(
        xScale * ix + xOffset,
        yScale * iy + yOffset
      ), 
      RangeData(values(ix)(iy))
    )
  
  /**
   * Evaluate this SampledFuction by applying scale and offset
   * to compute an index into the values array.
   * Note that this rounds to the nearest index (0.5 always rounds up)
   * to provide cell-centered semantics.
   */
  override def apply(
    value: DomainData, 
    interpolation: Interpolation = NoInterpolation(),
    extrapolation: Extrapolation = NoExtrapolation()
  ): Option[RangeData] = value match {
    //Note, adding the 0.5 then floor effectively rounds to the nearest index.
    //We could use "round" but it's not clear if rounding up at 0.5 is guaranteed.
    case DomainData(x: Double, y: Double) => 
      val ix = Math.floor((x - xOffset)/xScale + 0.5).toInt
      val iy = Math.floor((y - yOffset)/yScale + 0.5).toInt
      // Don't extrapolate. Return None if out of bounds.
      if (ix >= 0 && ix < nx && iy >= 0 && iy < ny) 
        Some(RangeData(values(ix)(iy)))
      else None
    case _ => ??? //TODO: error, invalid input
  }
}
