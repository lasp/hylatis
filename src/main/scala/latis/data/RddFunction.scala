package latis.data

import latis.util.SparkUtils._
import org.apache.spark.rdd.RDD
import fs2._
import cats.effect.IO
import latis.util.HylatisPartitioner
import org.apache.spark.rdd.PairRDDFunctions
import latis.resample._

/**
 * Implement SampledFunction by encapsulating a Spark RDD[Sample].
 * An RDD[Sample] is equivalent to an RDD[(DomainData, RangeData)]
 * which allows us to use the Spark PairRDDFunctions.
 */
case class RddFunction(rdd: RDD[Sample]) extends MemoizedFunction {
      
  override def apply(
    value: DomainData, 
    interpolation: Interpolation = NoInterpolation(),
    extrapolation: Extrapolation = NoExtrapolation()
  ): Option[RangeData] = {
    //TODO: support interpolation
    rdd.lookup(value).headOption
  }
  
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
    //TODO: no-op if empty
    // Gather the indices into the Domain of the Sample 
    // for the variables that are being grouped.
    val gbIndices = paths.map(_.head) map { case DomainPosition(n) => n }

    // Define a function to extract the new domain from a Sample
    val groupByFunction: Sample => DomainData = (sample: Sample) => sample match {
      case Sample(domain, _) => DomainData.fromSeq(gbIndices.map(domain(_)))
    }

    // Define an aggregation function to construct a (nested) Function from the newly grouped Samples
    val agg: (DomainData, Iterable[Sample]) => Sample =
      (domain: DomainData, samples: Iterable[Sample]) => {
        val ss = samples.toVector map {
          case Sample(domain, range) =>
            // Remove the groupBy variables from the domain
            Sample(
              DomainData.fromSeq(domain.zipWithIndex.filterNot(p => gbIndices.contains(p._2)).map(_._1)),
              range
            )
        }
        //val stream: Stream[IO, Sample] = Stream.eval(IO(ss)).flatMap(Stream.emits(_))
        Sample(domain, RangeData(SampledFunction.fromSeq(ss))) 
      }
        
    //val partitioner = new HylatisPartitioner(4)
    val rdd2 = rdd.groupBy(groupByFunction)  //TODO: look into PairRDDFunctions.aggregateByKey or PairRDDFunctions.reduceByKey
                  .map(p => agg(p._1, p._2))
                  .sortBy(s => s.domain)
    
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
  
  override def union(that: SampledFunction) = that match {
    case rf: RddFunction =>
      // Note, spark union does not remove duplicates
      //TODO: consider cogroup => RDD[(K, (Iterable[S], Iterable[S]))]
      RddFunction(this.rdd.union(rf.rdd).distinct.sortBy(identity))
    case _ => ??? //TODO: union expects both to be RddFunctions
  }
}

object RddFunction extends FunctionFactory {

  def fromSamples(samples: Seq[Sample]): MemoizedFunction =
    RddFunction(sparkContext.parallelize(samples))
    //TODO: "count" the data to load it; configurable "cache" option

  override def restructure(data: SampledFunction): MemoizedFunction = data match {
    case rf: RddFunction => rf //no need to restructure
    case _ => fromSamples(data.unsafeForce.samples)
  }
}