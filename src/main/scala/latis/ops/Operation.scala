package latis.ops

import latis.model._
import latis.metadata._
import latis.data._


/**
 * An Operation is essentially a function that takes a single Dataset
 * and returns a new Dataset
 */
trait Operation {
  //TODO: UnaryOperation?  we often fold over Seq[Operation] assuming unary
  
  /**
   * Provide an indication that this operation was applied 
   * for the provenance (history) metadata.
   */
  def provenance: String = this.toString //TODO: add time, version
  
  /**
   * Provide a new model resulting from this Operations.
   * Default to no-op.
   */
  def applyToModel(model: DataType): DataType = model
  
  /**
   * Provide new Data resulting from this Operation.
   * Note that we get the Dataset here and not just the input Data
   * since data operations often need the metadata/model.
   * Default to no-op.
   * TODO: make sure we get the original model
   */
  def applyToData(ds: Dataset): Data = ds.data
  
  /**
   * Provide new Metadata resulting from this Operation.
   * Add provenance then delegate to applyToModel.
   */
  def applyToMetadata(md: Metadata): Metadata = {
    val history = md.getProperty("history") match {
      case Some(h) => h + "\n" + provenance  //TODO: StringUtils.newline
      case None => provenance
    }
    val props = md.properties + ("history" -> history)
    Metadata(props)
  }
  
  /**
   * Apply this Operation to the given Dataset and provide a new Dataset
   * with updated Metadata and Data.
   */
  def apply(ds: Dataset): Dataset = ds match {
    case Dataset(md, model, data) => Dataset(applyToMetadata(ds.metadata), applyToModel(model), applyToData(ds))
  }
  
}