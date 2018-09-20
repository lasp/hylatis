package latis.ops

import latis.data._
import latis.metadata._
import latis.model._


/**
 * An Operation that maps a function of Sample => Sample
 * over the samples of a Dataset.
 */
trait MapOperation extends Operation {
  
  def makeMapFunction(model: DataType): Sample => Sample
  
  //def applyToModel(model: DataType): DataType = model
  
  /**
   * Provide new Data resulting from this Operation.
   * Note that we get the Dataset here and not just the input Data
   * since data operations often need the metadata/model.
   * Default to no-op.
   * TODO: make sure we get the original model
   */
  override def applyToData(ds: Dataset): SampledFunction = {
    val f = makeMapFunction(ds.model)
    val samples = ds.samples.map(f)
    //TODO: replicate orig Function impl
    StreamingFunction(samples)
  }
  
}