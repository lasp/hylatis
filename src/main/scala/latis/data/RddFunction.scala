package latis.data

import latis.util.SparkUtils._
import org.apache.spark.rdd.RDD
import fs2._
import cats.effect.IO
import latis.util.HylatisPartitioner

/**
 * Implement SampledFunction by encapsulating a Spark RDD[Sample].
 * TODO: should we treat this as Memoized?
 *   we started from Samples instead of Stream, so presumably ok
 */
case class RddFunction(rdd: RDD[Sample]) extends MemoizedFunction {
  
  /**
   * Cache the RDD every time we find it worthy to construct
   * an RddFunction out of it.
   * Use function composition before calling the methods here
   * to avoid excess caching.
   */
  rdd.cache()
  
  //TODO: unpersist orig RDD?
  
  override def apply(v: DomainData): RddFunction = ???
  
  override def samples: Seq[Sample] = 
    rdd.toLocalIterator.toSeq
  
  override def streamSamples: Stream[IO, Sample] = 
    Stream.fromIterator[IO, Sample](rdd.toLocalIterator)

    
  override def filter(p: Sample => Boolean): RddFunction = 
    RddFunction(rdd.filter(p))
    
  override def map(f: Sample => Sample): RddFunction =
    RddFunction(rdd.map(f))
 
  override def flatMap(f: Sample => MemoizedFunction): RddFunction =
    RddFunction(rdd.flatMap(s => f(s).samples))
    
  override def groupBy(paths: SamplePath*): RddFunction = {
    //Assume Hysics use case: (iy, ix, w) -> f  => (ix, iy) -> w -> f
    val groupByFunction: Sample => DomainData = (sample: Sample) => sample match {
      case Sample(DomainData(iy, ix, _), _) => DomainData(ix, iy)
      case _ => ??? //TODO: impl general case
    }
    
    val agg: (DomainData, Iterable[Sample]) => Sample = (data: DomainData, samples: Iterable[Sample]) => {
      val ss = samples.toVector map {
        case Sample(ds, rs) => Sample(ds.drop(2), rs)  // w -> f
      }
      //val stream: Stream[IO, Sample] = Stream.eval(IO(ss)).flatMap(Stream.emits(_))
      Sample(data, RangeData(SampledFunction.fromSeq(ss))) // (ix, iy) -> w -> f
    }
        
    val partitioner = new HylatisPartitioner(4)
    val rdd2 = rdd.groupBy(groupByFunction, partitioner)  //TODO: look into PairRDDFunctions.aggregateByKey or PairRDDFunctions.reduceByKey
                  .map(p => agg(p._1, p._2))
    
    /*
     * TODO: we got the new samples but they are not grouped, nor sorted
     * is this due to no equals/hashCode for DomainData (Array[Any])?
     */
                  
    RddFunction(rdd2)
  }
  
}

object RddFunction extends FunctionFactory {

  def fromSeq(samples: Seq[Sample]): MemoizedFunction =
    RddFunction(sparkContext.parallelize(samples))

}