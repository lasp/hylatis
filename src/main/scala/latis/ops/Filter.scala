package latis.ops

import latis.data._
import latis.metadata._
import latis.model._

/**
 * A Filter is a unary Operation that applys a boolean
 * predicate to each Sample of the given Dataset resulting
 * in a new Dataset that has all the "false" Samples removed.
 * This only impacts the number of Samples in the Dataset. 
 * It does not affect the model.
 * TODO: clarify behavior of nested Functions: all or none
 */
trait Filter extends Operation {
  
  def makePredicate(model: DataType): Sample => Boolean
  
  override def applyToData(ds: Dataset): SampledFunction = {
    //TODO: ds.filter(this)?
    
    val predicate = makePredicate(ds.model)
    
    val samples = ds.samples.filter(predicate)
    
    //TODO: reconcile "length"
    //TODO: preserve the Function data class used? Function.filter?
    StreamFunction(samples)
  }
}