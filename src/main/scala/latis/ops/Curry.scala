package latis.ops

import latis.data._
import latis.model._
import cats.effect.IO
import cats.kernel.Eq
import fs2.Stream

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
case class Curry(arity: Int = 1) extends UnaryOperation {
  //TODO: Provide description for prov
  //TODO: avoid adding prov if this is a no-op

  override def applyToModel(model: DataType): DataType = model.arity match {
    case n if n == arity => model //No-op, Dataset already has the desired arity
    case n if n < arity =>
      val msg = "Curry can only reduce arity"
      throw LatisException(msg)
    case _ => model match {
      case Function(tup: Tuple, range) =>
        val dts = tup.elements
        val domain =
          if (arity == 1) dts.head
          else Tuple(dts.take(arity))
        val innerDomain = dts.drop(arity) match {
          case seq if (seq.length == 1) => seq.head //reduce 1-tuple to scalar
          //TODO: prepend parent Tuple's id with "."
          case seq => Tuple(seq)
        }
        Function(domain, Function(innerDomain, range))
      //TODO: deal with named tuple
      //TODO: deal with nested tuple
      case _ =>
        // Bug if we get here
        val msg = s"Invalid dataset: $model"
        throw LatisException(msg)
    }
  }

  override def applyToData(data: SampledFunction, model: DataType): SampledFunction =
    model.arity match {
      case n if n == arity => data
      case n if n < arity =>
        val msg = "Curry can only reduce arity"
        throw LatisException(msg)
      case _ =>
        // Define a cats.Eq instance for Data to be used by Stream.groupAdjacentBy
        val eq: Eq[DomainData] = new cats.Eq[DomainData] {
          val scalars = model match {
            // Note, cases above imply that this should work
            case Function(d, _) => d.getScalars.take(arity)
          }
          val ord = LatisOrdering.domainOrdering(scalars)

          def eqv(a: DomainData, b: DomainData): Boolean = ord.equiv(a, b)
        }

        val extractCurriedValues: Sample => DomainData =
          (sample: Sample) => sample.domain.take(arity)

        val removeCurriedValues: Sample => Sample =
          (sample: Sample) => Sample(sample.domain.drop(arity), sample.range)

        // Chunk while the curried variables have the same value
        //   then combine the Samples in each chunk into a MemoizedFunction.
        val samples: Stream[IO, Sample] =
          data.streamSamples.groupAdjacentBy(extractCurriedValues)(eq) map {
            case (dd: DomainData, chunk) =>
              val nestedSamples = chunk.toList.map(removeCurriedValues)
              Sample(dd, RangeData(SampledFunction(nestedSamples)))
          }

        SampledFunction(samples)
    }
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

object Curry {

  val pipe: Stream[IO, Sample] => Stream[IO, Sample] =
    (in: Stream[IO, Sample]) => ???

}
