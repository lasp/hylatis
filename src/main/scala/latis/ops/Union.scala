package latis.ops

import latis.data._
import latis.model._

/**
 * Given two Datasets of the same type, 
 * combine the Samples into a new Dataset.
 * Note that because a Dataset's Samples are ordered, 
 * this union can happen as the data are being streamed.
 * Note, this does not affect the model.
 */
case class Union() extends BinaryOperation {
  
  /**
   * Returns the model of the first dataset 
   * since they are required to be the same.
   */
  def applyToModel(model1: DataType, model2: DataType): DataType = {
    //TODO require consistent models
    model1
  }
  
  /**
   * Combines the Data of two Datasets.
   */
  def applyToData(
    model1: DataType,
    data1: SampledFunction,
    model2: DataType,
    data2: SampledFunction
  ): SampledFunction = 
    data1 union data2

}