package latis.data

import latis.util.SparkUtils._
import org.apache.spark.rdd.RDD
import fs2._
import cats.effect.IO
import latis.util.HylatisPartitioner

/**
 * Implement SampledFunction by encapsulating a Spark RDD[Sample].
 */
case class RddFunction(rdd: RDD[Sample]) extends MemoizedFunction {
  
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
        
    //val partitioner = new HylatisPartitioner(4)
    implicit val ordering = DomainOrdering
    val rdd2 = rdd.groupBy(groupByFunction)  //TODO: look into PairRDDFunctions.aggregateByKey or PairRDDFunctions.reduceByKey
                  .map(p => agg(p._1, p._2))
                  .sortBy(s => s.domain)
                  
//  println(partitioner.numPartitions)
//  println(partitioner.getPartition(Seq(478,1)))
//  println(partitioner.getPartition(Seq(478,2101)))
//  println(partitioner.getPartition(Seq(479,1)))
//  println(partitioner.getPartition(Seq(479,2101)))
    
    /*
     * TODO: samples not sorted even with our partitioner
     * can we sort without shuffling if we use our partitioner?
     * what if more samples than partitions?
     * See OrderedRDDFunctions sortByKey
     * our partitioner should keep partitions ordered
     * sort within each partition - no shuffling?
     */
                  
    RddFunction(rdd2)
  }
  
}

object RddFunction extends FunctionFactory {

  def fromSamples(samples: Seq[Sample]): MemoizedFunction =
    RddFunction(sparkContext.parallelize(samples))

  override def restructure(data: SampledFunction): MemoizedFunction = data match {
    case rf: RddFunction => rf //no need to restructure
    case _ => fromSamples(data.unsafeForce.samples)
  }
}