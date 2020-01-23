package latis.data

import latis.util.LatisException


/**
 * Define a SampledFunction whose domain is defined
 * in terms of scale and offset.
 */
case class LinearFunction1D(scale: Double, offset: Double, values: Array[RangeData]) extends MemoizedFunction {
  //TODO: support Int, any numeric?
  
  /*
   * define SF with DomainSet (and Array, or RangeSet?)
   * LinearSet for DomainSet
   *   effectively defines CSX between index and value space
   *   should all DomainSets do that? see VisAD
   *   getIndex, nD? if Cartesian
   *   like intermediate step to evaluation
   *   evaluation could use domainSet's toIndex
   *   
   *   
   * CSX implications
   */
  
  /**
   * Provide a Seq of Samples with domain values computed
   * from scale and offset.
   */
  def sampleSeq: Seq[Sample] = {
    val n = values.length
    for {
      i <- (0 until n)
    } yield Sample(
      DomainData(scale * i + offset), 
      values(i)
    )
  }
  
  /**
   * Evaluate this SampledFuction by applying scale and offset
   * to compute an index into the values array.
   * Note that this rounds to the nearest index (0.5 always rounds up)
   * to provide cell-centered semantics.
   */
  //override def apply(value: DomainData): Either[LatisException, RangeData] = value match {
  //  //Note, adding the 0.5 then floor effectively rounds to the nearest index.
  //  //We could use "round" but it's not clear if rounding up at 0.5 is guaranteed.
  //  case DomainData(Number(x)) =>
  //    val index = Math.floor((x - offset)/scale + 0.5).toInt
  //    // Don't extrapolate. Return None if out of bounds.
  //    if (index >= 0 && index < values.length) Right(values(index))
  //    else {
  //      val msg = s"No sample found matching $value"
  //      Left(LatisException(msg))
  //    }
  //  case _ =>
  //    val msg = s"Invalid evaluation value for LinearFunction1D: $value"
  //    Left(LatisException(msg))
  //}
}
