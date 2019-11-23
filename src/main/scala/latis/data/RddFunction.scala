package latis.data

import latis.util.SparkUtils._
import org.apache.spark.rdd.RDD
import fs2._
import cats.effect.IO
import latis.util.HylatisPartitioner
import org.apache.spark.rdd.PairRDDFunctions
import latis.ops._
import org.apache.spark.HashPartitioner

/**
 * Implement SampledFunction by encapsulating a Spark RDD[Sample].
 * An RDD[Sample] is equivalent to an RDD[(DomainData, RangeData)]
 * which allows us to use the Spark PairRDDFunctions.
 */
case class RddFunction(rdd: RDD[Sample]) extends MemoizedFunction {
  //Note, PairRDDFunctions has an implicit Ordering[K] arg with default value of null
      
  override def apply(
    value: DomainData
  ): Option[RangeData] = {
    //TODO: support interpolation
    rdd.lookup(value).headOption
  }
  /*
   * lookup:
   * "This operation is done efficiently if the RDD has a known partitioner by only searching the partition that the key maps to."
   * one job per lookup
   * only from driver?
   * "if partitioner is None, spark will filter all"
   * we get a MapPartitionsRDD with a partitioner of None
   * as a PairRDD it does have our DomainOrdering but None partitioner
   */
  
  override def samples: Seq[Sample] = 
    rdd.toLocalIterator.toSeq
  
  override def streamSamples: Stream[IO, Sample] = 
    Stream.fromIterator[IO, Sample](rdd.toLocalIterator)

  //TODO: use mapValues... to avoid repartitioning
  // mapRange
    
  override def filter(p: Sample => Boolean): RddFunction = 
    RddFunction(rdd.filter(p))
    
  override def map(f: Sample => Sample): RddFunction =
    RddFunction(rdd.map(f))
    
  override def mapRange(f: RangeData => RangeData): RddFunction =
    RddFunction(rdd.mapValues(f))
 
  override def flatMap(f: Sample => MemoizedFunction): RddFunction =
    RddFunction(rdd.flatMap(s => f(s).samples))
    
    
  //TODO: look into PairRDDFunctions.aggregateByKey or PairRDDFunctions.reduceByKey
  //  use groupRange akin to mapRange 
  override def groupBy(
    groupByFunction: Sample => Option[DomainData], 
    aggregation: Aggregation = NoAggregation()
  )(implicit ordering: Ordering[DomainData]): MemoizedFunction = RddFunction {
    /*
     * TODO: optional Partitioner arg, default to this SF domainSet? but might not be cheap
     * use domainSet from GBB
     * but doesn't make sense for generic SF.groupBy to take a partitioner
     * override groupByBin?
     *   but lose generality
     *   would have to be on SF itself
     *   OK if we want that to be part of the FA, not just DSL
     * really just need min/max and use config'd number of partitions?
     *   RDD does have min, max
     */
    
/*
 * TODO: flatMap over Option[DomainData] from groupByFunction?
 * we could compose with the gbf to get past option
 *   how can we throw away outliers ?
 *   can we make a null DomainData bin where things go to die?
 *   the agg'r could do that
 *   would need to drop bogus key
 *   but can we avoid moving data?
 * will GB partition by the Option or wait until agg?
 *   if it waits, we can handle the Option in the agg
 * 
 * if we GB without option then we could use mapValues to avoid further reshuffling
 *   unless spark optimizes
 *     not sure how it could
 *     you have the GB everything before you can agg
 *     the samples have to go somewhere
 *   but agg still needs dd even if it doesn't change it
 * TODO: does this shuffle more than once?
 */
    // Modify the aggregation function to handle optional DomainData
    val agg: (Option[DomainData], Iterable[Sample]) => Option[Sample] =
      (odd: Option[DomainData], samples: Iterable[Sample]) => odd map {
        case dd => aggregation.aggregationFunction(dd, samples)
      }

    rdd.groupBy(groupByFunction) //RDD[(Option[DomainData], Iterable[Sample]]
       //.map(p => aggregation.aggregationFunction(p._1.get, p._2))
       .flatMap(p => agg(p._1, p._2))
       .sortBy(s => s.domain)
  }
  
    /*
     * Could groupBy be impl'd via flatMap?
     * RDD flatMap vs groupBy
     *   would we be giving up optimization of spark gb?
     * flatMap: S => TraversableOnce[S]
     *   "flattens" the Traversable
     *   SF would have to impl TraversableOnce
     *   what would it meen to flatten a SF? 
     *     no nested Functions?
     *     that sounds like what Aggregation could do
     *     do we always want that kind of flattening?
     * groupBy: S => (S, Iterable[S])
     */
   
  override def groupBy(paths: SamplePath*)(implicit ordering: Ordering[DomainData]): RddFunction = {
    //TODO: no-op if empty
    // Gather the indices into the Domain of the Sample 
    // for the variables that are being grouped.
    val gbIndices = paths.map(_.head) map { case DomainPosition(n) => n }

    // Define a function to extract the new domain from a Sample
    val groupByFunction: Sample => DomainData = (sample: Sample) => sample match {
      case Sample(domain, _) => DomainData(gbIndices.map(domain(_)))
    }

    // Define an aggregation function to construct a (nested) Function from the newly grouped Samples
    val agg: (DomainData, Iterable[Sample]) => Sample =
      (domain: DomainData, samples: Iterable[Sample]) => {
        val ss = samples.toVector map {
          case Sample(domain, range) =>
            // Remove the groupBy variables from the domain
            Sample(
              DomainData(domain.zipWithIndex.filterNot(p => gbIndices.contains(p._2)).map(_._1)),
              range
            )
        }
        //val stream: Stream[IO, Sample] = Stream.eval(IO(ss)).flatMap(Stream.emits(_))
        Sample(domain, RangeData(SampledFunction(ss))) 
      }
        
    //val partitioner = new HylatisPartitioner(4)
    val rdd2 = rdd.groupBy(groupByFunction)  //TODO: look into PairRDDFunctions.aggregateByKey or PairRDDFunctions.reduceByKey
                  .map(p => agg(p._1, p._2))
                  .sortBy(s => s.domain)
    /*
     * Can we use functions from GroupBy Operation here
     * with Aggregation
     *   GB gives us a new DomainSet
     *     for each DomainData we get a Seq (Iterable) of Samples which is what the rdd agg wants
     *     so we should be able to use the same GB function
     *     possibly compose it?
     *   Aggregator: samples => Vector[Data]
     *     rdd wants Sample
     *     we have the DD here so we can combine it with our agg output wrapped in RangeData
     * how can the Op delegate to the SF
     *   at lower level that "groupBy"
     *   akin to calling sf.map(f)
     *   sf.groupBy(gbf, aggf)?
     *   could we then compose csx with those?
     *     gbf: Sample => DomainData
     *     csx: DomainData => DomainData
     *  ** csx andThen gbf !
     * 
     * GroupByBin 
     * apply csx on the fly to avoid repartitioning
     * 
     * What about removing the vars we grouped by
     * can we "unproject" 
     * but the new domain will have the same variables
     * in general, a GB function could compute any domain, not just reshuffle
     *   f: Sample => DomainData
     */
                  
                  
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
    
  /**
   * Join two RddFunctions assuming they have the same domain set.
   */
  def join(that: RddFunction): RddFunction = {
    val rdd = (this.rdd join that.rdd) mapValues {
      case (r1: RangeData, r2: RangeData) => r1 ++ r2
    }
    RddFunction(rdd)
  }
  
  //override def union(that: SampledFunction) = that match {
  //  case rf: RddFunction =>
  //    // Note, spark union does not remove duplicates
  //    //TODO: consider cogroup => RDD[(K, (Iterable[S], Iterable[S]))]
  //    RddFunction(this.rdd.union(rf.rdd).distinct.sortBy(identity))
  //  case _ => ??? //TODO: union expects both to be RddFunctions
  //}
}

object RddFunction extends FunctionFactory {

  def fromSamples(samples: Seq[Sample]): MemoizedFunction = {
    // Put data into a Spark RDD[Sample] with a Partitioner
    // with the number of partitions set to the number of Samples.
    //TODO: try our Partitioner
    val part = new HashPartitioner(samples.length)
    val rdd = sparkContext.parallelize(samples)
                          .partitionBy(part)
    RddFunction(rdd)
  }
    

  override def restructure(data: SampledFunction): MemoizedFunction = data match {
    case rf: RddFunction => rf //no need to restructure
    case _ => fromSamples(data.unsafeForce.samples)
  }
}
