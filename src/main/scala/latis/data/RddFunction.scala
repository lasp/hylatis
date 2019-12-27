package latis.data

import latis.util.SparkUtils._
import org.apache.spark.rdd.RDD
import fs2._
import cats.effect.IO

import latis.util.HylatisPartitioner
import org.apache.spark.rdd.PairRDDFunctions

import latis.ops._
import org.apache.spark.HashPartitioner

import latis.model.DataType
import latis.ops.MapRangeOperation

/**
 * Implement SampledFunction by encapsulating a Spark RDD[Sample].
 * An RDD[Sample] is equivalent to an RDD[(DomainData, RangeData)]
 * which allows us to use the Spark PairRDDFunctions.
 */
case class RddFunction(rdd: RDD[Sample]) extends MemoizedFunction {
  //Note, PairRDDFunctions has an implicit Ordering[K] arg with default value of null
  //See OrderedRDDFunctions sortByKey

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
  
  override def sampleSeq: Seq[Sample] =
    rdd.toLocalIterator.toSeq
  
  override def samples: Stream[IO, Sample] =
    Stream.fromIterator[IO, Sample](rdd.toLocalIterator)


  override def applyOperation(op: UnaryOperation, model: DataType): SampledFunction = op match {
    case filter: Filter => RddFunction(rdd.filter(filter.predicate(model)))
    case MapOperation(f) => RddFunction(rdd.map(f(model)))
    case flatMapOp: FlatMapOperation => RddFunction(rdd.flatMap(flatMapOp.flatMapFunction(model)(_).sampleSeq))
    case mapRange: MapRangeOperation => RddFunction(rdd.mapValues(mapRange.mapFunction(model)))
    case groupOp: GroupOperation => RddFunction(
      rdd.groupBy(groupOp.groupByFunction(model)(_).get) //TODO: deal with None
        .mapValues(groupOp.aggregation.aggregateFunction(model)(_))
    )
    case _ => super.applyOperation(op, model)
  }


//
//  /**
//   * Join two RddFunctions assuming they have the same domain set.
//   */
//  def join(that: RddFunction): RddFunction = {
//    val rdd = (this.rdd join that.rdd) mapValues {
//      case (r1: RangeData, r2: RangeData) => r1 ++ r2
//    }
//    RddFunction(rdd)
//  }
  
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
    case _ => fromSamples(data.unsafeForce.sampleSeq)
  }
}
