package latis.data

import org.apache.spark.rdd.RDD
import fs2._
import cats.effect.IO
import latis.algebra.FunctionalAlgebra

class RddFuction(rdd: RDD[Sample]) extends SampledFunction 
  with FunctionalAlgebra[RDD] {
  
  def samples: Stream[IO, Sample] = 
    Stream.fromIterator[IO, Sample](rdd.toLocalIterator)
    
  def filter(p: Sample => Boolean): RDD[Sample] = 
    rdd.filter(p)
    
  def map(f: Sample => Sample): RDD[Sample] =
    rdd.map(f)
    
  
}