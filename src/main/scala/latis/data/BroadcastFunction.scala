package latis.data

import latis.util.SparkUtils

/**
 * SampledFunction with Samples broadcast over the Spark cluster as a
 * Seq[Sample].
 */
case class BroadcastFunction(private val _samples: Seq[Sample]) extends MemoizedFunction {
  
  private val broadcast = SparkUtils.sparkContext.broadcast(_samples)
  
  def samples: Seq[Sample] = broadcast.value
}
