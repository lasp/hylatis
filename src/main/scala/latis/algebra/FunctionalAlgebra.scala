package latis.algebra

import fs2._
import cats.effect.IO
import latis.data.Sample

/*
* impl by SampledFunction, Dataset, RDD, Adapter?
* impl all here as opposed to Operation?
*
*/
trait FunctionalAlgebra[F[_]] {

  /*
   * TODO: how to implement default?
   * base SampledFunction could implement them based on F = Stream
   * can Adapter concat a partial alg? might not be based on Stream
   * other SampledFunction subclasses could fall back to default
   *   how to concat RDD algebra with Stream?
   */
  
  /**
   * Implementing class needs to provide Stream of samples
   * to support default implementations.
   */
  //def samples: F[Sample]
  //TODO: would this be helpful if F is a Functor...?
  
  
  def filter(p: Sample => Boolean): F[Sample]
    //samples.filter(p)
    
  def map(f: Sample => Sample): F[Sample]
    //samples.map(f)
  
  
//--- Model and Data Agnostic Filters ---//
//TODO: may need to update coverage metadata
//take(n)
//takeRight(n)
//drop(n)
//dropRight(n)
//stride(n)
//head, first
//tail
//last, lastOption
//--- Filters by Variable ID ---//
//select(vid, op, value)
//takeWhile, takeUntil?
//--- Model Manipulation ---//
//rename(vid, vid2) //model only
//curry? //also modify samples but not values
//def project(vids: String*) //also modify samples but not values
//--- Data Manipulation ---//
//not likely to be impld by adapter
//replaceValue
//compute/derive
//bin, thin
//integrate
//...
//filter, map, flatMap
/*
* Too many to itemize, impl in terms of base algebra?
* many are just mapping a function
* like v3 beta ops: filter, mapping, flatMapping
* some SampledFunction impls could optimize by applying
* in parallel instead of a single stream of samples
*/
//memoize => MemoizedFunction, data in memory, release resources
}
