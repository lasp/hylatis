package latis.data

import cats.effect.IO
import fs2._
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner

import latis.model.DataType
import latis.ops._
import latis.ops.MapRangeOperation
import latis.util.HylatisPartitioner
import latis.util.LatisConfig
import latis.util.SparkUtils._

/**
 * Implement SampledFunction by encapsulating a Spark RDD[Sample].
 * An RDD[Sample] is equivalent to an RDD[(DomainData, RangeData)]
 * which allows us to use the Spark PairRDDFunctions.
 */
case class RddFunction(rdd: RDD[Sample]) extends MemoizedFunction {
  //Note, PairRDDFunctions has an implicit Ordering[K] arg with default value of null
  //See OrderedRDDFunctions sortByKey

  //TODO: just to make SF happy for now, make this work with Sample ordering
  def ordering: Option[PartialOrdering[DomainData]] = None

  //override def apply(value: DomainData): Either[LatisException, RangeData] = {
  //  //TODO: support interpolation
  //  rdd.lookup(value).headOption match {
  //    case Some(r) => Right(r)
  //    case None =>
  //      val msg = s"No sample found matching $value"
  //      Left(LatisException(msg))
  //  }
  //}
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
    //TODO: repartition as needed
    case filter: Filter => RddFunction(rdd.filter(filter.predicate(model)))
    case MapOperation(f) => RddFunction(rdd.map(f(model)))
    case flatMapOp: FlatMapOperation => RddFunction(rdd.flatMap(flatMapOp.flatMapFunction(model)(_).sampleSeq))
    case mapRange: MapRangeOperation => RddFunction(
      rdd.mapValues { rd =>
        RangeData(mapRange.mapFunction(model)(Data.fromSeq(rd)))
      }
    )
    case groupOp: GroupOperation => RddFunction(
      gb(groupOp, model)
      //rdd.groupBy(groupOp.groupByFunction(model)(_).get) //TODO: deal with None
      //  .mapValues(groupOp.aggregation.aggregateFunction(model)(_))
    )
    case _ => super.applyOperation(op, model)
  }
/*
TODO: GroupByBin needs to have a bin for each domainSet element, even if empty
  should it do another map to add empty (filled?) bins?
    would like to avoid shuffle
  could be done as a join?
    but how to generalized?
    could it avoid shuffle?
 */
  def gb(groupOp: GroupOperation, model: DataType): RDD[Sample] = {
    ////define fill RDD if GBB
    //val fillRDD: Option[RDD[_]] = {
    //  val samples = groupOp match {
    //    case GroupByBin(dset, _) =>
    //      dset.elements.map((_, RangeData(NullData)))
    //  }
    //  Some(sparkContext.parallelize(samples))
    //  //TODO: partition based on 1st dim
    //}

    // Defines a key for homeless samples using NullData
    val badKey: DomainData = {
      val n = groupOp.domainType(model).getScalars.length
      List.fill(n)(NullDatum)
    }

    val f: Sample => DomainData = groupOp.groupByFunction(model)(_).getOrElse(badKey)

    //TODO: use our partitioner
    val nPartitions = LatisConfig.getOrElse("spark.default.parallelism", 4)
    val p = new HashPartitioner(nPartitions) //Partitioner.defaultPartitioner(rdd)

    implicit val ord: Ordering[DomainData] = groupOp.ordering(model) //needed to be able to call SortByKey
    rdd.groupBy(f, p)
      .mapValues{ rd =>
        RangeData(groupOp.aggregation.aggregateFunction(model)(rd))
      }
      //.partitionBy(Partitioner.defaultPartitioner(z))
      .sortByKey() //apparently won't sort by itself, but impl ord does enable implicit OrderedRDDFunctions
      // Remove homeless samples
      //TODO: seems like there should be a more efficient way
      // take all but last, but only if last is null
      .filter {
        case Sample(d, _) if ord.equiv(d, badKey) => false
        case _ => true
      }
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
    import scala.math._
    // Put data into a Spark RDD[Sample] with a Partitioner
    // with the number of partitions set to the number of Samples.
    // Get min, max of first domain variable
    val (dmin, dmax) = {
      def go(ss: List[Sample], dmin: Double, dmax: Double): (Double, Double) = ss match {
        case Nil => (dmin, dmax)
        case s :: ss => s match {
          case Sample(DomainData(Number(d), _*), _) =>
            go(ss, min(dmin, d), max(dmax, d))
        }
      }
      go(samples.toList, Double.MaxValue, Double.MinValue)
    }
    val part = HylatisPartitioner(samples.length, dmin, dmax)
    val rdd = sparkContext.parallelize(samples)
                          .partitionBy(part)
    RddFunction(rdd)
  }
    

  override def restructure(data: SampledFunction): MemoizedFunction = data match {
    case rf: RddFunction => rf //no need to restructure
    case _ => fromSamples(data.unsafeForce.sampleSeq)
  }
}
