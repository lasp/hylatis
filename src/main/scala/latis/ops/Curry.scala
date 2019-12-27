package latis.ops

import cats.effect.IO
import cats.kernel.Eq
import fs2.Stream
import ucar.nc2.ncml.Aggregation

import latis.data._
import latis.model._
import latis.util.LatisException
import latis.util.LatisOrdering

/**
 * Curry a Dataset by taking the first variable of a multi-dimensional domain
 * and making it the new domain with the rest represented as a nested Function.
 * 
 * e.g. curry(a): (a, b) -> c  =>  a -> b -> c
 * 
 * The effect is the restructuring of Samples such that the primary (outer) 
 * Function has one Sample per curried variable value.
 * 
 * Note that this will be a no-op for Datasets that already have arity one.
 * Assume no named Tuples or nested Tuples in domain, for now.
 */
case class Curry(arity: Int = 1) extends GroupOperation {
  //TODO: Provide description for prov
  //TODO: avoid adding prov if this is a no-op
  /*
  TODO: fail fast if invalid for target dataset
    it would be handy if we constructed Ops with the model
      e.g. applyToData wouldn't require model
      but wouldn't make sense from a user perspective
        e.g. just say "curry(2)"
      could check when we do ds.withOp
        would need to compute current state of model, no biggie
      op.validateForModel(model)?
    could we have a secondary class of Op with model?
      but would mean that we can't re-order...

   */
  /*
  TODO: extend GroupOperation?
     need to override pipe to maintain order
     applyToModel would be the same as GBV, extend it?
       but would need to provide var names
       still not clear about use of agg, composed with unproject
       we need unproject built in here
       BGV wouldn't require it but might be the default
       applyToModel serves to provide exceptions if not initial variables
     how would this be applied to RDD?
       would it just work if it were a GroupOp?
   */
  def groupByFunction(model: DataType): Sample => Option[DomainData] = {
    if (model.arity < arity) {
      val msg = "Curry can only reduce arity"
      throw LatisException(msg)
    }
    (sample: Sample) => Some(sample.domain.take(arity))
  }

  def domainType(model: DataType): DataType = model match {
    case Function(Tuple(es @ _*), _) => Tuple(es.take(arity))
  }

  def aggregation: Aggregation = {
    val mapOp = new MapOperation {
      override def mapFunction(model: DataType): Sample => Sample =
        (sample: Sample) => sample match {
          case Sample(d, r) => Sample(d.drop(arity), r)
        }

      override def applyToModel(model: DataType): DataType = model match {
        case Function(Tuple(es @ _*), range) => Function(Tuple(es.drop(arity)), range)
          //TODO: beef up edge cases
      }
    }

    DefaultAggregation().compose(mapOp)
  }

  //override def applyToModel(model: DataType): DataType = model.arity match {
  //  case n if n == arity => model //No-op, Dataset already has the desired arity
  //  case n if n < arity =>
  //    val msg = "Curry can only reduce arity"
  //    throw LatisException(msg)
  //  case _ => model match {
  //    case Function(tup: Tuple, range) =>
  //      val dts = tup.elements
  //      val domain =
  //        if (arity == 1) dts.head
  //        else Tuple(dts.take(arity))
  //      val innerDomain = dts.drop(arity) match {
  //        case seq if (seq.length == 1) => seq.head //reduce 1-tuple to scalar
  //        //TODO: prepend parent Tuple's id with "."
  //        case seq => Tuple(seq)
  //      }
  //      Function(domain, Function(innerDomain, range))
  //    //TODO: deal with named tuple
  //    //TODO: deal with nested tuple
  //    case _ =>
  //      // Bug if we get here
  //      val msg = s"Invalid dataset: $model"
  //      throw LatisException(msg)
  //  }
  //}

  //TODO: override pipe to optimize for streaming, otherwise makes SortedMap

  //override def applyToData(data: SampledFunction, model: DataType): SampledFunction =
  //  model.arity match {
  //    case n if n == arity => data
  //    case n if n < arity =>
  //      val msg = "Curry can only reduce arity"
  //      throw LatisException(msg)
  //    case _ =>
  //      // Define a cats.Eq instance for Data to be used by Stream.groupAdjacentBy
  //      val eq: Eq[DomainData] = new cats.Eq[DomainData] {
  //        val scalars = model match {
  //          // Note, cases above imply that this should work
  //          case Function(d, _) => d.getScalars.take(arity)
  //        }
  //        val ord: PartialOrdering[DomainData] = LatisOrdering.domainOrdering(scalars)
  //
  //        def eqv(a: DomainData, b: DomainData): Boolean = ord.equiv(a, b)
  //      }
  //
  //      val extractCurriedValues: Sample => DomainData =
  //        (sample: Sample) => sample.domain.take(arity)
  //
  //      val removeCurriedValues: Sample => Sample =
  //        (sample: Sample) => Sample(sample.domain.drop(arity), sample.range)
  //
  //      // Chunk while the curried variables have the same value
  //      //   then combine the Samples in each chunk into a MemoizedFunction.
  //      val samples: Stream[IO, Sample] =
  //        data.samples.groupAdjacentBy(extractCurriedValues)(eq).map {
  //          case (dd: DomainData, chunk) =>
  //            val nestedSamples = chunk.toList.map(removeCurriedValues)
  //            Sample(dd, RangeData(SampledFunction(nestedSamples)))
  //        }
  //
  //      SampledFunction(samples)
  //  }
  /*
  TODO: factor out into SampledFunction such that RddFunction can override
  needs model to get scalars to get PartialOrdering to get Eq
  def curry(arity)(implicit pord: PartialOrdering[DomainData])
  def curry(arity, model)
  do via map in SF? even if RDD doesn't use map
    samples.map(Curry....)
    but not applied to just sample
    flatMap?
    f(stream)?
    applyToStream?
  default impl here needs to be static
  testable with a stream of samples
  Stream to Stream = Pipe!?
  FunctionalAlgebra trait with impl as pipes?
  extend by SF, FA with "samples: Stream"
  Dataset could also impl?
    but would also need to apply to model
    or better as implicit DatasetOps?
  could still put primary impl in Op?

  Should RddFunction be built with the model?
  curry in general doesn't need ordering

can applyToData be happy since applyToModel will happen first?

Consider that Dataset now orchestrates op application

   */
}
