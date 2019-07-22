package latis.data

/**
 * Define a SampledFunction whose domain is defined
 * in terms of scale and offset.
 */
case class LinearFunction1D(scale: Double, offset: Double, values: Array[Any]) extends MemoizedFunction {
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
  def samples: Seq[Sample] = {
    val n = values.length
    for {
      i <- (0 until n)
    } yield Sample(
      DomainData(scale * i + offset), 
      RangeData(values(i))
    )
  }
  
  /**
   * Evaluate this SampledFuction by applying scale and offset
   * to compute an index into the values array.
   * Note that this rounds to the nearest index (0.5 always rounds up)
   * to provide cell-centered semantics.
   */
  override def apply(value: DomainData): Option[RangeData] = value match {
    //Note, adding the 0.5 then floor effectively rounds to the nearest index.
    //We could use "round" but it's not clear if rounding up at 0.5 is guaranteed.
    case DomainData(x: Double) => 
      val index = Math.floor((x - offset)/scale + 0.5).toInt
      // Don't extrapolate. Return None if out of bounds.
      if (index >= 0 && index < values.length) Some(RangeData(values(index)))
      else None
    case _ => ??? //TODO: error, invalid input
  }
}
