package latis.data

import latis.util.SparkUtils

/**
 * SampledFunction with Samples broadcast over the Spark cluster as a
 * Seq[Sample].
 */
case class BroadcastFunction(private val _samples: Seq[Sample]) extends MemoizedFunction {
  //TODO: broadcast any MemoizedFunction, delegate to it?
  
  private val broadcast = SparkUtils.sparkContext.broadcast(_samples)
  
  def sampleSeq: Seq[Sample] = broadcast.value
}
