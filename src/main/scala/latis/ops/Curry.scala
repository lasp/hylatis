package latis.ops

import latis.data._
import latis.model._

import cats.effect.IO
import fs2.Stream

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
case class Curry() extends UnaryOperation {
  //TODO: support currying multiple variables
  //  e.g. (x, y, z) -> a  =>  (x, y) -> z -> a
  //TODO: Provide description for prov
  
  /**
   * Override to ensure the Dataset has the appropriate DataType
   * then delegate to super and proceed as normal.
   * If the dataset has arity one (one-dimensional domain) 
   *   or 0 (Scalar or Tuple) then there is nothing to do.
   */
  override def apply(ds: Dataset): Dataset = ds.model.arity match {
    case n if n < 2 => ds
    case _ => super.apply(ds)
  }
  
  override def applyToModel(model: DataType): DataType = model match {
    case Function(Tuple(dts @ _*), range) =>
      val domain = dts.head
      val innerDomain = dts.tail.length match {
        case 1 => dts.tail.head  //reduce 1-tuple to scalar 
          //TODO: prepend parent Tuple's id with "."
        case _ => Tuple(dts.tail: _*)
        //Note, 0 is not possible since we only apply (above) to data
      }
      Function(domain, Function(innerDomain, range))
      //TODO: deal with named tuple
      //TODO: deal with nested tuple
    case _ => ??? // shouldn't get here since arity > 1 implies a Tuple domain
  }
  
  override def applyToData(data: SampledFunction, model: DataType): SampledFunction = {
    // Define a cats.Eq instance for Any to be used by Stream.groupAdjacentBy
    val eq = new cats.Eq[Data] {
      def eqv(a: Data, b: Data): Boolean = (a, b) match {
        case (Number(a), Number(b)) => (a compare b) == 0
        case (Text(a), Text(b))     => (a compare b) == 0
        case _ => false
      }
    }
    
    // Chunk while the curried variable has the same value
    //   then combine the Samples in each chunk into a SampledFunction.
    val samples: Stream[IO, Sample] = 
      data.streamSamples.groupAdjacentBy(Curry.extractCurriedValue)(eq) map {
        case (d: Data, chunk) =>
          val nestedSamples = chunk.toList.map(Curry.removeCurriedValue)
          Sample(
            DomainData(d), 
            RangeData(SampledFunction(nestedSamples))
          )
      }
    
    SampledFunction(samples)
  }
  
}

object Curry {
  
  /**
   * Get the value of the curried variable (first value of the domain)
   * from a Sample.
   */
  val extractCurriedValue: Sample => Data = (sample: Sample) => 
    sample._1.head
      
  /**
   * Remove the curried value (first value of the domain)
   * from the given Sample and return a new Sample.
   */
  val removeCurriedValue: Sample => Sample = (sample: Sample) => 
    Sample(sample._1.tail, sample._2)
}
