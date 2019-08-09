package latis.ops

import latis.model.Dataset
import latis.data._

/**
 * Given two Datasets of the same type, 
 * combine the Samples into a new Dataset.
 * Note that because a Dataset's Samples are ordered, 
 * this union can happen as the data are being streamed.
 * Note, this does not affect the model.
 */
case class Union() extends BinaryOperation {
  
  def apply(ds1: Dataset, ds2: Dataset): Dataset = {
    
    val metadata = ds1.metadata //TODO: combine metadata, prov
    
    val model = ds1.model //TODO: ensure the models match
    
    val data: SampledFunction = ds1.data union ds2.data
    
    Dataset(metadata, model, data)
  }
}